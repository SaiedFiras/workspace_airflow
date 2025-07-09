from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 1),
    'retries': 1
}

with DAG(
    dag_id='stg_to_ods',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    # Drop et creation des tables ODS
    drop_ods_tables = SQLExecuteQueryOperator(
        task_id="drop_ods_tables",
        conn_id="postgres",
        sql=""" 
        DROP TABLE IF EXISTS ods_clients CASCADE;
        DROP TABLE IF EXISTS ods_posts CASCADE;
        DROP TABLE IF EXISTS ods_insights CASCADE;
        DROP TABLE IF EXISTS ods_comments CASCADE;
        """
    )
    create_ods_tables = SQLExecuteQueryOperator(
        task_id="create_ods_tables",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS ods_clients (
            client_id INT PRIMARY KEY,
            name VARCHAR,
            managed INT,
            social_type VARCHAR,
            actif INT
        );
        
        CREATE TABLE IF NOT EXISTS ods_posts (
            post_id BIGINT PRIMARY KEY,
            client_id INT,
            social_type VARCHAR,
            post_type VARCHAR,
            creation_time TIMESTAMP,
            message TEXT,
            likes INT,
            shares INT,
            views INT,
            reactions INT,
            is_real VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS ods_insights (
            insight_id BIGINT PRIMARY KEY,
            client_id INT,
            insight_date DATE,
            fans INT,
            new_fans INT,
            unfollows INT,
            engaged_fans INT,
            impressions INT,
            organic_impressions INT,
            paid_impressions INT,
            post_engagements INT,
            follows INT,
            fans_online INT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        """
    )

    # Tables ODS (Transformation brutes a propres)
    ods_clients = SQLExecuteQueryOperator(
        task_id="ods_clients",
        conn_id="postgres",
        sql="""
        INSERT INTO ods_clients (client_id, name, managed, social_type, actif)
        SELECT * FROM (
            SELECT 
                id AS client_id,
                name,
                managed,
                social_type,
                actif
            FROM stg_clients
            UNION
            SELECT
                id AS client_id,
                name,
                managed,
                social_type,
                actif
            FROM stg_unmanaged_clients
            WHERE id NOT IN (SELECT id FROM stg_clients)
        ) AS FUSION
        ON CONFLICT (client_id) DO NOTHING;
        """
    )


    ods_posts = SQLExecuteQueryOperator(
        task_id="ods_posts",
        conn_id="postgres",
        sql="""
        INSERT INTO ods_posts
        SELECT 
            id AS post_id,
            page_id AS client_id,
            social_type,
            type AS post_type,
            creation_time,
            message,
            likes,
            shares,
            views,
            reactions,
            is_real,
            created_at,
            updated_at
        FROM stg_posts
        WHERE is_deleted = 0
        ON CONFLICT (post_id) DO NOTHING;
        """
    )


    ods_insights = SQLExecuteQueryOperator(
        task_id="ods_insights",
        conn_id="postgres",
        sql="""
        INSERT INTO ods_insights
        SELECT 
            id AS insight_id,
            page_id AS client_id,
            day AS insight_date,
            page_fans AS fans,
            page_fan_adds AS new_fans,
            page_fan_removes AS unfollows,
            page_engaged_users AS engaged_fans,
            page_impressions_unique AS impressions,
            page_impressions_organic_unique_v2 AS organic_impressions,
            page_impressions_paid_unique AS paid_impressions,
            page_post_engagements AS post_engagements,
            page_follows AS follows,
            page_fans_online_per_day AS fans_online,
            created_at,
            updated_at
        FROM stg_insights
        ON CONFLICT (insight_id) DO NOTHING;
        """
    )

    # Dependances de taches
    (
      drop_ods_tables >> create_ods_tables >> ods_clients >> ods_posts >> ods_insights
    )  
