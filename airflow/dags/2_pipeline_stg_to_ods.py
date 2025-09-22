from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
        task_id="truncate_ods_tables",
        conn_id="postgres",
        sql=""" 
        TRUNCATE TABLE ods_clients CASCADE;
        TRUNCATE TABLE ods_posts CASCADE;
        TRUNCATE TABLE ods_insights CASCADE;
        TRUNCATE TABLE ods_fans_country CASCADE;
        TRUNCATE TABLE ods_fans_city CASCADE;
        TRUNCATE TABLE ods_fans_locale CASCADE;
        TRUNCATE TABLE ods_data_video;
        """
    )

    # create_ods_tables = SQLExecuteQueryOperator(
    #     task_id="create_ods_tables",
    #     conn_id="postgres",
    #     sql="""
        
    #     CREATE TABLE IF NOT EXISTS ods_clients (
    #         client_id INT PRIMARY KEY,
    #         name VARCHAR,
    #         managed BOOLEAN,
    #         social_type VARCHAR,
    #         actif BOOLEAN,
    #         type VARCHAR,
    #         picture TEXT
    #     );
        
    #     CREATE TABLE IF NOT EXISTS ods_posts (
    #         post_id BIGINT PRIMARY KEY,
    #         client_id INT,
    #         social_type VARCHAR,
    #         post_type VARCHAR,
    #         creation_time TIMESTAMP,
    #         message TEXT,
    #         comments INT,
    #         likes INT,
    #         shares INT,
    #         views INT,
    #         clicks INT,
    #         reactions INT,
    #         reach INT,
    #         reach_organic INT,
    #         reach_paid INT,
    #         is_real VARCHAR,
    #         post_clicks_by_type TEXT,
    #         FOREIGN KEY (client_id) REFERENCES ods_clients(client_id)
    #     );

    #     CREATE TABLE IF NOT EXISTS ods_insights (
    #         insight_id BIGINT PRIMARY KEY,
    #         client_id INT,
    #         insight_date DATE,
    #         page_fans BIGINT,
    #         page_fan_adds INTEGER,
    #         page_fan_removes INTEGER,
    #         page_engaged_users INTEGER,
    #         page_impressions_unique INTEGER,
    #         page_impressions_organic_unique_v2 INTEGER,
    #         page_impressions_paid_unique INTEGER,
    #         page_post_engagements INTEGER,
    #         page_fans_online_per_day INTEGER,
    #         page_impressions INTEGER,
    #         page_impressions_organic_v2 INTEGER,
    #         page_impressions_paid INTEGER,
    #         page_follows BIGINT,
    #         page_fans_country TEXT,
    #         page_fans_city TEXT,
    #         page_fans_locale TEXT,
    #         FOREIGN KEY (client_id) REFERENCES ods_clients(client_id)
    #     );

    #     CREATE TABLE IF NOT EXISTS ods_fans_country (
    #         insight_id BIGINT,
    #         client_id INT,
    #         insight_date DATE,
    #         country VARCHAR,
    #         nb_fans INT,
    #         PRIMARY KEY (insight_id, country),
    #         FOREIGN KEY (insight_id) REFERENCES ods_insights(insight_id),
    #         FOREIGN KEY (client_id) REFERENCES ods_clients(client_id)
    #     );
    #     CREATE TABLE IF NOT EXISTS ods_fans_city (
    #         insight_id BIGINT,
    #         client_id INT,
    #         insight_date DATE,
    #         city VARCHAR,
    #         nb_fans INT,
    #         PRIMARY KEY (insight_id, city),
    #         FOREIGN KEY (insight_id) REFERENCES ods_insights(insight_id),
    #         FOREIGN KEY (client_id) REFERENCES ods_clients(client_id)
    #     );
    #     CREATE TABLE IF NOT EXISTS ods_fans_locale (
    #         insight_id BIGINT,
    #         client_id INT,
    #         insight_date DATE,
    #         locale VARCHAR,
    #         nb_fans INT,
    #         PRIMARY KEY (insight_id, locale),
    #         FOREIGN KEY (insight_id) REFERENCES ods_insights(insight_id),
    #         FOREIGN KEY (client_id) REFERENCES ods_clients(client_id)
    #     );

    #     CREATE TABLE IF NOT EXISTS ods_data_video (
    #         post_id INTEGER PRIMARY KEY,
    #         total_video_views INTEGER,
    #         total_video_views_paid INTEGER,
    #         total_video_views_organic INTEGER,
    #         total_video_views_autoplayed INTEGER,
    #         total_video_views_clicked_to_play INTEGER,
    #         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #         FOREIGN KEY (post_id) REFERENCES ods_posts(post_id)
    #     );
    #     """
    # )

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
                    WHEN type = 'storie' THEN 'Story'
                    WHEN url IS NOT NULL AND url LIKE '/reel/%' THEN 'Reels'
                    WHEN status_type = 'added_photos' THEN 'Photo'
                    WHEN status_type = 'added_video' THEN 'Video'
                    WHEN status_type = 'mobile_status_update' THEN 'Reels'
                    WHEN status_type IN ('shared_story', 'storie_photo', 'storie_video') THEN 'Story'
                    WHEN (type IS NULL OR type = '') AND (status_type IS NULL OR status_type = '') THEN 'Texte'
                    ELSE NULL
                END
            ) AS post_type,
            creation_time,
            message,
            comments,
            likes,
            shares,
            views,
            clicks,
            COALESCE(likes, 0) + COALESCE(wow, 0) + COALESCE(sad, 0) + COALESCE(haha, 0) +
            COALESCE(angry, 0) + COALESCE(love, 0) + COALESCE(thankful, 0) AS reactions,
            reach,
            reach_organic,
            reach_paid,
            is_real
        FROM stg_posts
        WHERE is_deleted = 0
            AND page_id IN (
                SELECT id FROM stg_clients
                WHERE LOWER(social_type) NOT IN ('ytb', 'youtube', 'tiktok', 'linkedin')
            )
            OR page_id IN (
                SELECT id FROM stg_unmanaged_clients
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
            page_follows,
            page_fans_country TEXT,
            page_fans_city TEXT,
            page_fans_locale TEXT
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
            o.insight_id AS insight_id,
            o.client_id AS client_id,
            o.insight_date AS insight_date,
            key AS country,
            value::INT AS nb_fans
        FROM ods_insights o 
        CROSS JOIN LATERAL json_each_text(
            CASE
                WHEN o.page_fans_country IS NOT NULL AND o.page_fans_country != '' AND LEFT(TRIM(o.page_fans_country), 1) != '{'
                THEN '{' || o.page_fans_country || '}'
                ELSE o.page_fans_country
            END::json
        )
        WHERE o.page_fans_country IS NOT NULL 
            AND o.page_fans_country NOT IN ('0', '[]', '{}') 
            AND o.page_fans_country != ''
        ON CONFLICT (insight_id, country) DO NOTHING;
        """
    )
    ods_fans_city = SQLExecuteQueryOperator(
        task_id="ods_fans_city",
        conn_id="postgres",
        sql="""
        INSERT INTO ods_fans_city (insight_id, client_id, insight_date, city, nb_fans)
        SELECT
            o.insight_id AS insight_id,
            o.client_id AS client_id,
            o.insight_date AS insight_date,
            key AS city,
            value::INT AS nb_fans
        FROM ods_insights o 
        CROSS JOIN LATERAL json_each_text(
            CASE
                WHEN o.page_fans_city IS NOT NULL AND o.page_fans_city != '' AND LEFT(TRIM(o.page_fans_city), 1) != '{'
                THEN '{' || o.page_fans_city || '}'
                ELSE o.page_fans_city
            END::json
        )
        WHERE o.page_fans_city IS NOT NULL
            AND o.page_fans_city NOT IN ('0', '[]', '{}') 
            AND o.page_fans_city != ''
        ON CONFLICT (insight_id, city) DO NOTHING;
        """
    )
    ods_fans_locale = SQLExecuteQueryOperator(
        task_id="ods_fans_locale",
        conn_id="postgres",
        sql="""
        INSERT INTO ods_fans_locale (insight_id, client_id, insight_date, locale, nb_fans)
        SELECT
            o.insight_id AS insight_id,
            o.client_id AS client_id,
            o.insight_date AS insight_date,
            key AS locale,
            value::INT AS nb_fans
        FROM ods_insights o
        CROSS JOIN LATERAL json_each_text(
            CASE
                WHEN o.page_fans_locale IS NULL OR TRIM(o.page_fans_locale) = ''
                    THEN NULL
                WHEN LEFT(TRIM(o.page_fans_locale), 1) = '{' AND RIGHT(TRIM(o.page_fans_locale), 2) = '}}'
                    THEN LEFT(TRIM(o.page_fans_locale), LENGTH(TRIM(o.page_fans_locale))-1)
                WHEN LEFT(TRIM(o.page_fans_locale), 1) = '{' AND RIGHT(TRIM(o.page_fans_locale), 1) = '}'
                    THEN TRIM(o.page_fans_locale)
                WHEN LEFT(TRIM(o.page_fans_locale), 1) <> '{' AND RIGHT(TRIM(o.page_fans_locale), 1) = '}'
                    THEN '{' || TRIM(o.page_fans_locale)
                WHEN LEFT(TRIM(o.page_fans_locale), 1) = '{' AND RIGHT(TRIM(o.page_fans_locale), 1) <> '}'
                    THEN TRIM(o.page_fans_locale) || '}'
                ELSE '{' || TRIM(o.page_fans_locale) || '}'
            END::json
        )
        WHERE o.page_fans_locale IS NOT NULL
            AND o.page_fans_locale NOT IN ('0', '[]', '{}')
            AND o.page_fans_locale != ''
        ON CONFLICT (insight_id, locale) DO NOTHING;
        """
    )

    ods_data_video = SQLExecuteQueryOperator(
        task_id="ods_data_video",
        conn_id="postgres",
        sql="""
        INSERT INTO ods_data_video (
            post_id,
            total_video_views,
            total_video_views_paid,
            total_video_views_organic,
            total_video_views_autoplayed,
            total_video_views_clicked_to_play
        )
        SELECT
            p.id AS post_id,
            (p.data::json->'data'->>'total_video_views')::INT AS total_video_views,
            (p.data::json->'data'->>'total_video_views_paid')::INT AS total_video_views_paid,
            (p.data::json->'data'->>'total_video_views_organic')::INT AS total_video_views_organic,
            (p.data::json->'data'->>'total_video_views_autoplayed')::INT AS total_video_views_autoplayed,
            (p.data::json->'data'->>'total_video_views_clicked_to_play')::INT AS total_video_views_clicked_to_play
        FROM stg_posts p
        WHERE p.data IS NOT NULL
            AND p.data NOT IN ('0', '[]', '{}') 
            AND p.data != ''
        ON CONFLICT (post_id) DO NOTHING;
        """
    )

    

    clean_null = SQLExecuteQueryOperator(
        task_id="clean_all_null",
        conn_id="postgres",
        sql="""
        -- Nettoyage Colonne Post_type dans ods_posts
        UPDATE ods_posts
        SET post_type = 'Story' 
        WHERE post_type = 'storie';

        -- Nettoyage de ods_clients
        UPDATE ods_clients
        SET name = NULLIF(TRIM(name), ''),
            type = NULLIF(TRIM(type), ''),
            picture = NULLIF(TRIM(picture), '');

        -- Nettoyage de ods_posts
        UPDATE ods_posts
        SET message = NULLIF(TRIM(message), ''),
            post_type = NULLIF(TRIM(post_type), ''),
            is_real = NULLIF(TRIM(is_real), '');

        -- Nettoyage de ods_insights
        UPDATE ods_insights
        SET page_fans = NULLIF(page_fans, 0),
            page_fan_adds = NULLIF(page_fan_adds, 0),
            page_fan_removes = NULLIF(page_fan_removes, 0),
            page_engaged_users = NULLIF(page_engaged_users, 0),
            page_impressions_unique = NULLIF(page_impressions_unique, 0),
            page_impressions_organic_unique_v2 = NULLIF(page_impressions_organic_unique_v2, 0),
            page_impressions_paid_unique = NULLIF(page_impressions_paid_unique, 0),
            page_post_engagements = NULLIF(page_post_engagements, 0),
            page_fans_online_per_day = NULLIF(page_fans_online_per_day, 0),
            page_impressions = NULLIF(page_impressions, 0),
            page_impressions_organic_v2 = NULLIF(page_impressions_organic_v2, 0),
            page_impressions_paid = NULLIF(page_impressions_paid, 0),
            page_follows = NULLIF(page_follows, 0);

        -- Nettoyage de ods_fans_country, city, locale
        UPDATE ods_fans_country SET country = NULLIF(TRIM(country), '') WHERE TRUE;
        UPDATE ods_fans_city SET city = NULLIF(TRIM(city), '') WHERE TRUE;
        UPDATE ods_fans_locale SET locale = NULLIF(TRIM(locale), '') WHERE TRUE;
        """
    )

    trigger_ods_to_dw = TriggerDagRunOperator(
    task_id="trigger_ods_to_dw",
    trigger_dag_id="ods_to_dw",
    wait_for_completion=False,
    dag=dag
    )


    (
        drop_ods_tables >> 
        ods_clients >> ods_posts >> ods_insights >>
        ods_fans_country >> ods_fans_city >> ods_fans_locale >>
        ods_data_video >>
        clean_null >>
        trigger_ods_to_dw
    )

#create_ods_tables >>