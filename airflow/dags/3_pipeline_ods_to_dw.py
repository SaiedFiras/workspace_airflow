from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1
}

with DAG(
    dag_id='ods_to_dw',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. DROP/CREATE ALL TABLES
    drop_dw_tables = SQLExecuteQueryOperator(
        task_id="truncate_dw_tables",
        conn_id="postgres",
        sql="""
        DROP TABLE IF EXISTS fact_posts CASCADE;
        DROP TABLE IF EXISTS fact_insights CASCADE;
        DROP TABLE IF EXISTS fact_fan_demographics CASCADE;
        DROP TABLE IF EXISTS dim_client CASCADE;
        DROP TABLE IF EXISTS dim_post_type CASCADE;
        DROP TABLE IF EXISTS dim_date CASCADE;
        DROP TABLE IF EXISTS dim_location CASCADE;
        """
    )

    create_dw_tables = SQLExecuteQueryOperator(
        task_id="create_dw_tables",
        conn_id="postgres",
        sql="""
        -- Dimension Client
        CREATE TABLE IF NOT EXISTS dim_client (
            client_id INT PRIMARY KEY,
            name VARCHAR,
            managed BOOLEAN,
            social_type VARCHAR,
            actif BOOLEAN,
            type VARCHAR,
            picture TEXT
        );

        -- Dimension Post Type
        CREATE TABLE IF NOT EXISTS dim_post_type (
            post_type_id SERIAL PRIMARY KEY,
            label VARCHAR UNIQUE
        );

        -- Dimension Date
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id SERIAL PRIMARY KEY,
            full_date DATE UNIQUE,
            year INT,
            month INT,
            day INT,
            week INT,
            quarter INT
        );

        -- Dimension Location (pays, ville, langue)
        CREATE TABLE IF NOT EXISTS dim_location (
            location_id SERIAL PRIMARY KEY,
            location_type VARCHAR, -- 'country', 'city', 'locale'
            location_label VARCHAR,
            UNIQUE(location_type, location_label)
        );

        -- Fait Posts (posts, vidéos, reels…)
        CREATE TABLE IF NOT EXISTS fact_posts (
            post_id BIGINT PRIMARY KEY,
            client_id INT REFERENCES dim_client(client_id),
            date_id INT REFERENCES dim_date(date_id),
            post_type_id INT REFERENCES dim_post_type(post_type_id),
            social_type VARCHAR,
            likes INT,
            comments INT,
            shares INT,
            views INT,
            clicks INT,
            reactions INT,
            reach INT,
            reach_organic INT,
            reach_paid INT,
            total_video_views INT,
            total_video_views_paid INT,
            total_video_views_organic INT,
            total_video_views_autoplayed INT,
            total_video_views_clicked_to_play INT,
            photo_view INT,
            link_clicks INT,
            other_clicks INT,
            is_real VARCHAR
        );

        -- Fait Insights (KPI par jour/client)
        CREATE TABLE IF NOT EXISTS fact_insights (
            insight_id BIGINT PRIMARY KEY,
            client_id INT REFERENCES dim_client(client_id),
            date_id INT REFERENCES dim_date(date_id),
            page_fans BIGINT,
            page_fan_adds INT,
            page_fan_removes INT,
            page_engaged_users INT,
            page_impressions_unique INT,
            page_impressions_organic_unique_v2 INT,
            page_impressions_paid_unique INT,
            page_post_engagements INT,
            page_fans_online_per_day INT,
            page_impressions INT,
            page_impressions_organic_v2 INT,
            page_impressions_paid INT,
            page_follows BIGINT
        );

        -- Fait Démographie unifiée
        CREATE TABLE IF NOT EXISTS fact_fan_demographics (
            fact_id SERIAL PRIMARY KEY,
            insight_id BIGINT,
            client_id INT REFERENCES dim_client(client_id),
            date_id INT REFERENCES dim_date(date_id),
            location_id INT REFERENCES dim_location(location_id),
            nb_fans INT
        );

        """
    )

    # 2. DIMENSIONS

    # Date
    dim_date = SQLExecuteQueryOperator(
        task_id="dim_date",
        conn_id="postgres",
        sql="""
        INSERT INTO dim_date (full_date, year, month, day, week, quarter)
        SELECT
            gs::date,
            EXTRACT(YEAR FROM gs)::INT,
            EXTRACT(MONTH FROM gs)::INT,
            EXTRACT(DAY FROM gs)::INT,
            EXTRACT(WEEK FROM gs)::INT,
            EXTRACT(QUARTER FROM gs)::INT
        FROM generate_series('2010-01-01'::date, CURRENT_DATE, interval '1 day') as gs
        ON CONFLICT (full_date) DO NOTHING;
        """
    )

    # Client
    dim_client = SQLExecuteQueryOperator(
        task_id="dim_client",
        conn_id="postgres",
        sql="""
        INSERT INTO dim_client (client_id, name, managed, social_type, actif, type, picture)
        SELECT DISTINCT
            client_id, name, managed, social_type, actif, type, picture
        FROM ods_clients
        ON CONFLICT (client_id) DO UPDATE 
            SET name=EXCLUDED.name, managed=EXCLUDED.managed, social_type=EXCLUDED.social_type, actif=EXCLUDED.actif, type=EXCLUDED.type, picture=EXCLUDED.picture;
        """
    )

    # Post Type
    dim_post_type = SQLExecuteQueryOperator(
        task_id="dim_post_type",
        conn_id="postgres",
        sql="""
        INSERT INTO dim_post_type(label)
        SELECT DISTINCT post_type FROM ods_posts
        WHERE post_type IS NOT NULL
        ON CONFLICT (label) DO NOTHING;
        """
    )

    # Location (fusionne pays, ville, langue)
    dim_location = SQLExecuteQueryOperator(
        task_id="dim_location",
        conn_id="postgres",
        sql="""
        INSERT INTO dim_location(location_type, location_label)
        SELECT 'country', country FROM ods_fans_country
        UNION
        SELECT 'city', city FROM ods_fans_city
        UNION
        SELECT 'locale', locale FROM ods_fans_locale
        ON CONFLICT (location_type, location_label) DO NOTHING;
        """
    )

    # 3. TABLES DE FAITS

    # POSTS
    fact_posts = SQLExecuteQueryOperator(
        task_id="fact_posts",
        conn_id="postgres",
        sql="""
        INSERT INTO fact_posts (
            post_id, client_id, date_id, post_type_id, social_type, comments, likes, shares, views, clicks,
            reactions, reach, reach_organic, reach_paid, is_real,
            total_video_views, total_video_views_paid, total_video_views_organic, 
            total_video_views_autoplayed, total_video_views_clicked_to_play, 
            photo_view, link_clicks, other_clicks 
        )
        SELECT
            o.post_id,
            o.client_id,
            d.date_id,
            pt.post_type_id,
            o.social_type,
            o.comments,
            o.likes,
            o.shares,
            o.views,
            o.clicks,
            o.reactions,
            o.reach,
            o.reach_organic,
            o.reach_paid,
            o.is_real,
            v.total_video_views,
            v.total_video_views_paid,
            v.total_video_views_organic,
            v.total_video_views_autoplayed,
            v.total_video_views_clicked_to_play,
            (o.post_clicks_by_type::json->>'photo view')::int,
            (o.post_clicks_by_type::json->>'link clicks')::int,
            (o.post_clicks_by_type::json->>'other clicks')::int
        FROM ods_posts o
        LEFT JOIN dim_date d ON o.creation_time::date = d.full_date
        LEFT JOIN dim_post_type pt ON o.post_type = pt.label
        LEFT JOIN ods_data_video v ON o.post_id = v.post_id
        ON CONFLICT (post_id) DO NOTHING;
        """
    )

    # INSIGHTS
    fact_insights = SQLExecuteQueryOperator(
        task_id="fact_insights",
        conn_id="postgres",
        sql="""
        INSERT INTO fact_insights (
            insight_id, client_id, date_id, page_fans, page_fan_adds, page_fan_removes, page_engaged_users,
            page_impressions_unique, page_impressions_organic_unique_v2, page_impressions_paid_unique,
            page_post_engagements, page_fans_online_per_day, page_impressions, page_impressions_organic_v2,
            page_impressions_paid, page_follows
        )
        SELECT
            o.insight_id,
            o.client_id,
            d.date_id,
            o.page_fans,
            o.page_fan_adds,
            o.page_fan_removes,
            o.page_engaged_users,
            o.page_impressions_unique,
            o.page_impressions_organic_unique_v2,
            o.page_impressions_paid_unique,
            o.page_post_engagements,
            o.page_fans_online_per_day,
            o.page_impressions,
            o.page_impressions_organic_v2,
            o.page_impressions_paid,
            o.page_follows
        FROM ods_insights o
        LEFT JOIN dim_date d ON o.insight_date = d.full_date
        ON CONFLICT (insight_id) DO NOTHING;
        """
    )
    

    # FAN DEMOGRAPHICS (UNIFICATION)
    fact_fan_demographics = SQLExecuteQueryOperator(
        task_id="fact_fan_demographics",
        conn_id="postgres",
        sql="""
        -- Pays
        INSERT INTO fact_fan_demographics (insight_id, client_id, date_id, location_id, nb_fans)
        SELECT
            c.insight_id,
            c.client_id,
            d.date_id,
            l.location_id,
            c.nb_fans
        FROM ods_fans_country c
        JOIN dim_location l ON l.location_type = 'country' AND l.location_label = c.country
        JOIN dim_date d ON d.full_date = c.insight_date

        UNION ALL

        -- Villes
        SELECT
            c.insight_id,
            c.client_id,
            d.date_id,
            l.location_id,
            c.nb_fans
        FROM ods_fans_city c
        JOIN dim_location l ON l.location_type = 'city' AND l.location_label = c.city
        JOIN dim_date d ON d.full_date = c.insight_date

        UNION ALL

        -- Langues
        SELECT
            c.insight_id,
            c.client_id,
            d.date_id,
            l.location_id,
            c.nb_fans
        FROM ods_fans_locale c
        JOIN dim_location l ON l.location_type = 'locale' AND l.location_label = c.locale
        JOIN dim_date d ON d.full_date = c.insight_date

        ON CONFLICT DO NOTHING;
        """
    )

    # Dependancs
    (
        drop_dw_tables >> 
        # create_dw_tables >>
        dim_date >> dim_client >> dim_post_type >> dim_location >>
        fact_posts >> fact_insights >> fact_fan_demographics
    )