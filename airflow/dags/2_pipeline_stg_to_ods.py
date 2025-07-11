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

    drop_ods_tables = SQLExecuteQueryOperator(
        task_id="drop_ods_tables",
        conn_id="postgres",
        sql=""" 
        DROP TABLE IF EXISTS ods_clients CASCADE;
        DROP TABLE IF EXISTS ods_posts CASCADE;
        DROP TABLE IF EXISTS ods_insights CASCADE;
        DROP TABLE IF EXISTS ods_fans_country CASCADE;
        DROP TABLE IF EXISTS ods_fans_city CASCADE;
        DROP TABLE IF EXISTS ods_fans_locale CASCADE;
        """
    )

    create_ods_tables = SQLExecuteQueryOperator(
        task_id="create_ods_tables",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS ods_clients (
            client_id INT PRIMARY KEY,
            name VARCHAR,
            managed BOOLEAN,
            social_type VARCHAR,
            actif BOOLEAN,
            type VARCHAR,
            picture TEXT
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
            page_fans BIGINT,
            page_fan_adds INTEGER,
            page_fan_removes INTEGER,
            page_engaged_users INTEGER,
            page_impressions_unique INTEGER,
            page_impressions_organic_unique_v2 INTEGER,
            page_impressions_paid_unique INTEGER,
            page_post_engagements INTEGER,
            page_fans_online_per_day INTEGER,
            page_impressions INTEGER,
            page_impressions_organic_v2 INTEGER,
            page_impressions_paid INTEGER,
            page_follows BIGINT
        );

        CREATE TABLE IF NOT EXISTS ods_fans_country (
            insight_id BIGINT,
            client_id INT,
            insight_date DATE,
            country VARCHAR,
            nb_fans INT,
            PRIMARY KEY (insight_id, country)
        );
        CREATE TABLE IF NOT EXISTS ods_fans_city (
            insight_id BIGINT,
            client_id INT,
            insight_date DATE,
            city VARCHAR,
            nb_fans INT,
            PRIMARY KEY (insight_id, city)
        );
        CREATE TABLE IF NOT EXISTS ods_fans_locale (
            insight_id BIGINT,
            client_id INT,
            insight_date DATE,
            locale VARCHAR,
            nb_fans INT,
            PRIMARY KEY (insight_id, locale)
        );
        """
    )

    ods_clients = SQLExecuteQueryOperator(
        task_id="ods_clients",
        conn_id="postgres",
        sql="""
        INSERT INTO ods_clients (client_id, name, managed, social_type, actif, type, picture)
        SELECT 
            id AS client_id,
            INITCAP(name) AS name,
            managed::BOOLEAN,
            social_type,
            actif::BOOLEAN,
            type,
            picture
        FROM stg_clients
        WHERE LOWER(social_type) NOT IN ('ytb', 'youtube', 'tiktok', 'linkedin')
        UNION ALL
        SELECT
            id AS client_id,
            INITCAP(name) AS name,
            managed::BOOLEAN,
            social_type,
            actif::BOOLEAN,
            type,
            picture
        FROM stg_unmanaged_clients
        WHERE LOWER(social_type) NOT IN ('ytb', 'youtube', 'tiktok', 'linkedin')
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
            COALESCE(
                NULLIF(type, ''), 
                CASE 
                    WHEN status_type = 'added_photos' THEN 'photo'
                    WHEN status_type = 'added_video' THEN 'video'
                    WHEN status_type = 'mobile_status_update' THEN 'reels'
                    WHEN status_type = 'shared_story' THEN 'story'
                    WHEN status_type = 'storie_photo' THEN 'story'
                    WHEN status_type = 'storie_video' THEN 'story'
                    WHEN type = 'storie' THEN 'story'
                    ELSE NULL
                END
            ) AS post_type,
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
            AND page_id IN (
                SELECT id FROM stg_clients
                WHERE LOWER(social_type) NOT IN ('ytb', 'youtube', 'tiktok', 'linkedin')
            )
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
            page_fans,
            page_fan_adds,
            page_fan_removes,
            page_engaged_users,
            page_impressions_unique,
            page_impressions_organic_unique_v2,
            page_impressions_paid_unique,
            page_post_engagements,
            page_fans_online_per_day,
            page_impressions,
            page_impressions_organic_v2,
            page_impressions_paid,
            page_follows
        FROM stg_insights
        WHERE page_id IN (
            SELECT id FROM stg_clients
            WHERE LOWER(social_type) NOT IN ('ytb', 'youtube', 'tiktok', 'linkedin')
        )
        ON CONFLICT (insight_id) DO NOTHING;
        """
    )

    ods_fans_country = SQLExecuteQueryOperator(
        task_id="ods_fans_country",
        conn_id="postgres",
        sql="""
        INSERT INTO ods_fans_country (insight_id, client_id, insight_date, country, nb_fans)
        SELECT
            s.id AS insight_id,
            s.page_id AS client_id,
            s.day AS insight_date,
            key AS country,
            value::INT AS nb_fans
        FROM stg_insights s, 
             LATERAL json_each_text(s.page_fans_country)
        WHERE s.page_fans_country IS NOT NULL
        ON CONFLICT (insight_id, country) DO NOTHING;
        """
    )
    ods_fans_city = SQLExecuteQueryOperator(
        task_id="ods_fans_city",
        conn_id="postgres",
        sql="""
        INSERT INTO ods_fans_city (insight_id, client_id, insight_date, city, nb_fans)
        SELECT
            s.id AS insight_id,
            s.page_id AS client_id,
            s.day AS insight_date,
            key AS city,
            value::INT AS nb_fans
        FROM stg_insights s, 
             LATERAL json_each_text(s.page_fans_city)
        WHERE s.page_fans_city IS NOT NULL
        ON CONFLICT (insight_id, city) DO NOTHING;
        """
    )
    ods_fans_locale = SQLExecuteQueryOperator(
        task_id="ods_fans_locale",
        conn_id="postgres",
        sql="""
        INSERT INTO ods_fans_locale (insight_id, client_id, insight_date, locale, nb_fans)
        SELECT
            s.id AS insight_id,
            s.page_id AS client_id,
            s.day AS insight_date,
            key AS locale,
            value::INT AS nb_fans
        FROM stg_insights s, 
             LATERAL json_each_text(s.page_fans_locale)
        WHERE s.page_fans_locale IS NOT NULL
        ON CONFLICT (insight_id, locale) DO NOTHING;
        """
    )

    (
        drop_ods_tables >> create_ods_tables >>
        ods_clients >> ods_posts >> ods_insights >>
        ods_fans_country >> ods_fans_city >> ods_fans_locale
    )
