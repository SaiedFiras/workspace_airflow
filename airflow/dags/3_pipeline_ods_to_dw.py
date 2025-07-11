from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 2),
    'retries': 1
}

with DAG(
    dag_id='ods_to_dw',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # SUPPRESSION DES TABLES SI ELLES EXITENT
    drop_all_tables = SQLExecuteQueryOperator(
        task_id="drop_all_tables",
        conn_id="postgres",
        sql=""" 
        DROP TABLE IF EXISTS ods_clients CASCADE;
        DROP TABLE IF EXISTS ods_posts CASCADE;
        DROP TABLE IF EXISTS ods_insights CASCADE;
        DROP TABLE IF EXISTS ods_comments CASCADE;
        DROP TABLE IF EXISTS dim_client CASCADE;
        DROP TABLE IF EXISTS dim_post_type CASCADE;
        DROP TABLE IF EXISTS dim_date CASCADE;
        DROP TABLE IF EXISTS fact_insights CASCADE;
        DROP TABLE IF EXISTS fact_posts CASCADE;
        DROP TABLE IF EXISTS fact_comments CASCADE;
        """
        )

    # 1. CREATION DES TABLES
    create_all_tables = SQLExecuteQueryOperator(
        task_id="create_all_tables",
        conn_id="postgres",
        sql="""
        -- ODS
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

        CREATE TABLE IF NOT EXISTS ods_comments (
            comment_id VARCHAR PRIMARY KEY,
            post_id INTEGER,
            client_id INTEGER,
            message TEXT,
            sentiment VARCHAR,
            user_name VARCHAR,
            created_at TIMESTAMP
        );

        -- DIMENSIONS DWH
        CREATE TABLE IF NOT EXISTS dim_client (
            client_id INT PRIMARY KEY,
            name VARCHAR,
            managed INT,
            social_type VARCHAR,
            actif INT
        );

        CREATE TABLE IF NOT EXISTS dim_post_type (
            post_type_id SERIAL PRIMARY KEY,
            label VARCHAR UNIQUE
        );

        CREATE TABLE IF NOT EXISTS dim_date (
            date_id SERIAL PRIMARY KEY,
            full_date DATE UNIQUE,
            year INT,
            month INT,
            day INT,
            week INT,
            quarter INT
        );

        -- FAITS DWH
        CREATE TABLE IF NOT EXISTS fact_insights (
            insight_id SERIAL PRIMARY KEY,
            client_id INT REFERENCES dim_client(client_id),
            date_id INT REFERENCES dim_date(date_id),
            social_type VARCHAR,
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
            nb_posts INT,
            total_likes INT,
            total_shares INT,
            total_comments INT,
            total_interactions INT
        );

        CREATE TABLE IF NOT EXISTS fact_posts (
            post_id BIGINT PRIMARY KEY,
            client_id INT REFERENCES dim_client(client_id),
            date_id INT REFERENCES dim_date(date_id),
            post_type_id INT REFERENCES dim_post_type(post_type_id),
            social_type VARCHAR,
            message TEXT,
            likes INT,
            shares INT,
            views INT,
            reactions INT,
            is_real VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );

        
        CREATE TABLE IF NOT EXISTS fact_comments (
            comment_id VARCHAR PRIMARY KEY,
            post_id BIGINT REFERENCES fact_posts(post_id),
            client_id INT REFERENCES dim_client(client_id),
            date_id INT REFERENCES dim_date(date_id),
            message TEXT,
            sentiment VARCHAR,
            user_name VARCHAR,
            created_at TIMESTAMP
        );
        """
    )

    # 2. ODS
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
            FROM clients
            UNION
            SELECT
                id AS client_id,
                name,
                managed,
                social_type,
                actif
            FROM unmanaged_clients
            WHERE id NOT IN (SELECT id FROM clients)
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
        FROM posts
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
        FROM insights
        ON CONFLICT (insight_id) DO NOTHING;
        """
    )
    ods_comments = SQLExecuteQueryOperator(
        task_id="ods_comments",
        conn_id="postgres",
        sql="""
        INSERT INTO ods_comments (comment_id, post_id, client_id, message, sentiment, user_name, created_at)
        SELECT
            c.id,
            c.post_id,
            p.client_id,  -- on récupère le client à partir du post
            c.message,
            c.sentiment,
            c.from_name,
            c.created_time
        FROM comments c
        JOIN ods_posts p ON c.post_id = p.post_id
        ON CONFLICT (comment_id) DO NOTHING;
        """
    )

    # 3. DIMENSIONS
    dim_client = SQLExecuteQueryOperator(
        task_id="dim_client",
        conn_id="postgres",
        sql="""
        INSERT INTO dim_client (client_id, name, managed, social_type, actif)
        SELECT DISTINCT client_id, name, managed, social_type, actif FROM ods_clients
        ON CONFLICT (client_id) DO UPDATE SET 
            name=EXCLUDED.name, managed=EXCLUDED.managed, social_type=EXCLUDED.social_type, actif=EXCLUDED.actif;
        """
    )
    dim_post_type = SQLExecuteQueryOperator(
        task_id="dim_post_type",
        conn_id="postgres",
        sql="""
        INSERT INTO dim_post_type (label)
        SELECT DISTINCT post_type FROM ods_posts
        WHERE post_type IS NOT NULL
        ON CONFLICT (label) DO NOTHING;
        """
    )
    dim_date = SQLExecuteQueryOperator(
        task_id="dim_date",
        conn_id="postgres",
        sql="""
        INSERT INTO dim_date (full_date, year, month, day, week, quarter)
        SELECT
            gs::date as full_date,
            EXTRACT(YEAR FROM gs)::INT,
            EXTRACT(MONTH FROM gs)::INT,
            EXTRACT(DAY FROM gs)::INT,
            EXTRACT(WEEK FROM gs)::INT,
            EXTRACT(QUARTER FROM gs)::INT
        FROM generate_series('2010-01-01'::date, CURRENT_DATE, interval '1 day') as gs
        ON CONFLICT (full_date) DO NOTHING;
        """
    )

    # 4. FAITS
    fact_insights = SQLExecuteQueryOperator(
        task_id="fact_insights",
        conn_id="postgres",
        sql="""
        INSERT INTO fact_insights (
            client_id, date_id, social_type, fans, new_fans, unfollows, engaged_fans, impressions,
            organic_impressions, paid_impressions, post_engagements, follows, fans_online,
            nb_posts, total_likes, total_shares, total_comments, total_interactions
        )
        SELECT
            i.client_id,
            d.date_id,
            c.social_type,
            MAX(i.fans),
            COALESCE(SUM(i.new_fans),0),
            COALESCE(SUM(i.unfollows),0),
            COALESCE(SUM(i.engaged_fans),0),
            COALESCE(SUM(i.impressions),0),
            COALESCE(SUM(i.organic_impressions),0),
            COALESCE(SUM(i.paid_impressions),0),
            COALESCE(SUM(i.post_engagements),0),
            COALESCE(SUM(i.follows),0),
            COALESCE(SUM(i.fans_online),0),
            -- NB POSTS à ce jour (pour ce client et ce réseau à la date donnée)
            (SELECT COUNT(p.post_id)
             FROM ods_posts p
             WHERE p.client_id = i.client_id
               AND p.social_type = c.social_type
               AND DATE(p.creation_time) = i.insight_date
            ) as nb_posts,
            (SELECT SUM(p.likes)
             FROM ods_posts p
             WHERE p.client_id = i.client_id
               AND p.social_type = c.social_type
               AND DATE(p.creation_time) = i.insight_date
            ) as total_likes,
            (SELECT SUM(p.shares)
             FROM ods_posts p
             WHERE p.client_id = i.client_id
               AND p.social_type = c.social_type
               AND DATE(p.creation_time) = i.insight_date
            ) as total_shares,
            (SELECT COUNT(cm.comment_id)
               FROM ods_comments cm
               JOIN ods_posts p2 ON cm.post_id = p2.post_id
               WHERE p2.client_id = i.client_id
                 AND p2.social_type = c.social_type
                 AND DATE(p2.creation_time) = i.insight_date
            ) as total_comments,
            COALESCE(
                (SELECT SUM(p.likes)
                 FROM ods_posts p
                 WHERE p.client_id = i.client_id
                   AND p.social_type = c.social_type
                   AND DATE(p.creation_time) = i.insight_date),0)
              + COALESCE((SELECT SUM(p.shares)
                 FROM ods_posts p
                 WHERE p.client_id = i.client_id
                   AND p.social_type = c.social_type
                   AND DATE(p.creation_time) = i.insight_date),0)
              + COALESCE((SELECT COUNT(cm.comment_id)
                 FROM ods_comments cm
                 JOIN ods_posts p2 ON cm.post_id = p2.post_id
                 WHERE p2.client_id = i.client_id
                   AND p2.social_type = c.social_type
                   AND DATE(p2.creation_time) = i.insight_date),0)
            as total_interactions
        FROM ods_insights i
        JOIN dim_client c ON i.client_id = c.client_id
        JOIN dim_date d ON i.insight_date = d.full_date
        GROUP BY i.client_id, d.date_id, c.social_type, i.insight_date
        ON CONFLICT DO NOTHING;
        """
    )

    fact_posts = SQLExecuteQueryOperator(
        task_id="fact_posts",
        conn_id="postgres",
        sql="""
        INSERT INTO fact_posts (
            post_id, client_id, date_id, post_type_id, social_type, message, likes, shares, views, reactions, is_real, created_at, updated_at
        )
        SELECT
            o.post_id,
            o.client_id,
            d.date_id,
            pt.post_type_id,
            o.social_type,
            o.message,
            o.likes,
            o.shares,
            o.views,
            o.reactions,
            o.is_real,
            o.created_at,
            o.updated_at
        FROM ods_posts o
        JOIN dim_date d ON o.creation_time::date = d.full_date
        LEFT JOIN dim_post_type pt ON o.post_type = pt.label
        ON CONFLICT (post_id) DO NOTHING;
        """
    )

    fact_comments = SQLExecuteQueryOperator(
        task_id="fact_comments",
        conn_id="postgres",
        sql="""
        INSERT INTO fact_comments (
            comment_id, post_id, client_id, date_id, message, sentiment, user_name, created_at
        )
        SELECT
            c.comment_id,
            c.post_id,
            c.client_id,
            d.date_id,
            c.message,
            c.sentiment,
            c.user_name,
            c.created_at
        FROM ods_comments c
        JOIN dim_date d ON c.created_at::date = d.full_date
        ON CONFLICT (comment_id) DO NOTHING;
        """
    )

    # Dépendances des taches du dag
    (
    drop_all_tables >> create_all_tables >> 
    ods_clients >> ods_posts >> ods_insights >> ods_comments >> 
    dim_client >> dim_post_type >> dim_date >> 
    fact_insights >> fact_posts >> fact_comments
    )