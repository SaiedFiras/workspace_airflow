from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from datetime import datetime
import requests
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 1),
    'retries': 1,
}

dag = DAG(
    'API_JSON_to_stg',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Drop tables pour permettre l'évolution du modèle si jamais je modifie les tables
drop_all_table = SQLExecuteQueryOperator(
    task_id='drop_all_stg_table',
    conn_id='postgres',
    sql="""
        DROP TABLE IF EXISTS stg_clients CASCADE;
        DROP TABLE IF EXISTS stg_posts CASCADE;
        DROP TABLE IF EXISTS stg_unmanaged_clients CASCADE;
        DROP TABLE IF EXISTS stg_insights CASCADE;
    """
)

create_stg_clients_table = SQLExecuteQueryOperator(
    task_id='create_stg_clients_table',
    conn_id='postgres',
    sql="""
        CREATE TABLE IF NOT EXISTS stg_clients (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            social_id VARCHAR,
            social_type VARCHAR,
            type VARCHAR,
            state TEXT,
            picture TEXT,
            cover TEXT,
            related_instagram_id VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            owner_id INTEGER,
            local_media TEXT,
            actif INTEGER,
            managed INTEGER
        );
    """,
    dag=dag,
)

create_stg_unmanaged_clients_table = SQLExecuteQueryOperator(
    task_id='create_stg_unmanaged_clients_table',
    conn_id='postgres',
    sql="""
        CREATE TABLE IF NOT EXISTS stg_unmanaged_clients (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            social_id VARCHAR,
            social_type VARCHAR,
            type VARCHAR,
            state TEXT,
            picture TEXT,
            cover TEXT,
            related_instagram_id VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            owner_id INTEGER,
            local_media TEXT,
            actif INTEGER,
            managed INTEGER
        );
    """,
    dag=dag,
)

create_stg_posts_table = SQLExecuteQueryOperator(
    task_id='create_stg_posts_table',
    conn_id='postgres',
    sql="""
        CREATE TABLE IF NOT EXISTS stg_posts (
            id BIGINT PRIMARY KEY,
            page_id INTEGER,
            social_id VARCHAR,
            social_type VARCHAR,
            creation_time TIMESTAMP,
            url TEXT,
            type VARCHAR,
            status_type VARCHAR,
            message TEXT,
            story TEXT,
            picture TEXT,
            profile_cover TEXT,
            reactions INTEGER,
            comments INTEGER,
            shares INTEGER,
            views INTEGER,
            views_organic INTEGER,
            views_paid INTEGER,
            reach INTEGER,
            reach_organic INTEGER,
            reach_paid INTEGER,
            likes INTEGER,
            wow INTEGER,
            sad INTEGER,
            haha INTEGER,
            angry INTEGER,
            none INTEGER,
            love INTEGER,
            thankful INTEGER,
            is_deleted INTEGER,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            data TEXT,
            local_media TEXT,
            clicks INTEGER,
            post_clicks_by_type TEXT,
            saved INTEGER,
            is_real VARCHAR,
            comments_ad INTEGER,
            views_ad INTEGER,
            reach_ad INTEGER,
            likes_ad INTEGER,
            saved_ad INTEGER,
            shares_ad INTEGER,
            tag VARCHAR,
            reactions_new TEXT,
            score_new VARCHAR,
            commentaires TEXT    
        );
    """,
    dag=dag,
)

create_stg_insights_table = SQLExecuteQueryOperator(
    task_id='create_stg_insights_table',
    conn_id='postgres',
    sql="""
        CREATE TABLE IF NOT EXISTS stg_insights (
            id BIGINT PRIMARY KEY,
            page_id INTEGER,
            day DATE,
            page_fans BIGINT,
            page_fan_adds INTEGER,
            page_fan_removes INTEGER,
            page_impressions_unique INTEGER,
            page_impressions_organic_unique_v2 INTEGER,
            page_impressions_paid_unique INTEGER,
            page_fans_online_per_day INTEGER,
            page_engaged_users INTEGER,
            page_fans_online TEXT,
            page_fans_country TEXT,
            page_fans_city TEXT,
            page_fans_locale TEXT,
            page_fans_gender_age TEXT,
            data TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            page_fans_seniority TEXT,
            page_fans_industry TEXT,
            page_fans_job_function TEXT,
            page_fan_adds_by_paid_non_paid_unique TEXT,
            page_post_engagements INTEGER,
            page_impressions INTEGER,
            page_impressions_paid INTEGER,
            page_impressions_organic_v2 INTEGER,
            page_follows BIGINT,
            page_impressions_unique_28 BIGINT,
            page_impressions_unique_week BIGINT
        );
    """,
    dag=dag,
)

def remove_nul_chars(val): # Suppression des caracteres nuls 
    if isinstance(val, str):
        return val.replace('\x00', '')
    return val

def fill_managed_stg_tables(**kwargs):
    managed_json = "/opt/airflow/datasets/export-all-managed-pages.json" #Le fichier des clients managé 
    conn_details = BaseHook.get_connection('postgres') #Le hook pour se connecter a la base postgres
    engine = create_engine(f'postgresql+psycopg2://{conn_details.login}:{conn_details.password}@{conn_details.host}:{conn_details.port}/{conn_details.schema}')

    with engine.connect() as connection:
        with open(managed_json, "r", encoding="utf-8") as f:
            managed_data = json.load(f)['data']
        for client in managed_data:
            client_id = client['id']
            url = f"https://api.diggow.com/api/export-page/{client_id}"
            try:
                response = requests.get(url, timeout=15)
                if response.status_code != 200:
                    print(f"Client managed {client_id} not found or error.")
                    continue
                data = response.json()
                if data and data['success'] and data['data']:
                    client_data = data['data']['page']
                    insights_data = data['data']['insights']
                    posts_data = data['data'].get('posts', [])
                    # Clients managed dans stg_clients
                    columns = ', '.join(client_data.keys())
                    values = ', '.join(['%s'] * len(client_data))
                    update = ', '.join(f"{col} = EXCLUDED.{col}" for col in client_data.keys())
                    sql = f"INSERT INTO stg_clients ({columns}) VALUES ({values}) ON CONFLICT (id) DO UPDATE SET {update}"
                    connection.execute(sql, tuple(client_data.values()))
                    # Insights
                    for insight in insights_data:
                        columns = ', '.join(insight.keys())
                        values = ', '.join(['%s'] * len(insight))
                        update = ', '.join(f"{col} = EXCLUDED.{col}" for col in insight.keys())
                        sql = f"INSERT INTO stg_insights ({columns}) VALUES ({values}) ON CONFLICT (id) DO UPDATE SET {update}"
                        connection.execute(sql, tuple(insight.values()))
                    # Posts
                    for post in posts_data:
                        columns = ', '.join(post.keys())
                        values = ', '.join(['%s'] * len(post))
                        update = ', '.join(f"{col} = EXCLUDED.{col}" for col in post.keys())
                        sql = f"INSERT INTO stg_posts ({columns}) VALUES ({values}) ON CONFLICT (id) DO UPDATE SET {update}"
                        connection.execute(sql, tuple(post.values()))
            except Exception as e:
                print(f"Error managed {client_id}: {e}")

def fill_unmanaged_stg_tables(**kwargs):
    unmanaged_json = "/opt/airflow/datasets/unmanagedpages.json"
    conn_details = BaseHook.get_connection('postgres')
    engine = create_engine(f'postgresql+psycopg2://{conn_details.login}:{conn_details.password}@{conn_details.host}:{conn_details.port}/{conn_details.schema}')

    with engine.connect() as connection:
        with open(unmanaged_json, "r", encoding="utf-8") as f:
            unmanaged_data = json.load(f)['data']
        for client in unmanaged_data:
            if 'page' in client:
                client_id = client['page']['id']
            else:
                client_id = client['id']
            url = f"https://api.diggow.com/api/export-page-unmanaged/{client_id}"
            try:
                response = requests.get(url, timeout=15)
                if response.status_code != 200:
                    print(f"Client unmanaged {client_id} not found or error.")
                    continue
                data = response.json()
                if data and data['success'] and data['data']:
                    client_data = data['data']['page']
                    insights_data = data['data']['insights']
                    posts_data = data['data'].get('posts', [])
                    # Clients unmanaged dans stg_unmanaged_clients
                    columns = ', '.join(client_data.keys())
                    values = ', '.join(['%s'] * len(client_data))
                    update = ', '.join(f"{col} = EXCLUDED.{col}" for col in client_data.keys())
                    sql = f"INSERT INTO stg_unmanaged_clients ({columns}) VALUES ({values}) ON CONFLICT (id) DO UPDATE SET {update}"
                    connection.execute(sql, tuple(client_data.values()))
                    # Insights
                    for insight in insights_data:
                        columns = ', '.join(insight.keys())
                        values = ', '.join(['%s'] * len(insight))
                        update = ', '.join(f"{col} = EXCLUDED.{col}" for col in insight.keys())
                        sql = f"INSERT INTO stg_insights ({columns}) VALUES ({values}) ON CONFLICT (id) DO UPDATE SET {update}"
                        connection.execute(sql, tuple(insight.values()))
                    # Posts
                    for post in posts_data:
                        columns = ', '.join(post.keys())
                        values = ', '.join(['%s'] * len(post))
                        update = ', '.join(f"{col} = EXCLUDED.{col}" for col in post.keys())
                        sql = f"INSERT INTO stg_posts ({columns}) VALUES ({values}) ON CONFLICT (id) DO UPDATE SET {update}"
                        connection.execute(sql, tuple(post.values()))
            except Exception as e:
                print(f"Error unmanaged {client_id}: {e}")

fill_managed_stg_tables_task = PythonOperator(
    task_id='fill_managed_stg_tables',
    python_callable=fill_managed_stg_tables,
    dag=dag,
)

fill_unmanaged_stg_tables_task = PythonOperator(
    task_id='fill_unmanaged_stg_tables',
    python_callable=fill_unmanaged_stg_tables,
    dag=dag,
)

trigger_stg_to_ods = TriggerDagRunOperator(
    task_id="trigger_stg_to_ods",
    trigger_dag_id="stg_to_ods",
    wait_for_completion=False,
    dag=dag
)

# Dépendances
(
    drop_all_table >>
    create_stg_clients_table >>
    create_stg_unmanaged_clients_table >>
    create_stg_posts_table >>
    create_stg_insights_table >>
    fill_managed_stg_tables_task >>
    fill_unmanaged_stg_tables_task >>
    trigger_stg_to_ods
)




"""
Ici nous allons essayer d'automatiser les liens de chaque clients via l'api,
mais il y a des ID clients qui n'existe pas, et les ids ne suive pas une suite arithmétique 
(Exemple : 5 client, 22 client, 1 aucun client)

valid_client_ids = [] # clients valide stocker dans un tableau
max_client_id = 100  # plage des clients id a check

for client_id in range(1, max_client_id + 1): # itération pour check l'existence pour chaque id jusqu'a 100
    try:
        get_data_task = PythonOperator(
            task_id=f'get_data_{client_id}',
            python_callable=get_api_data,
            op_kwargs={'client_id': client_id},
            dag=dag,
        )

        insert_data_task = PythonOperator(
            task_id=f'insert_data_{client_id}',
            python_callable=insert_data_to_postgres,
            op_kwargs={'client_id': client_id},
            dag=dag,
        )

        create_clients_table >> create_insights_table >> get_data_task >> insert_data_task
        valid_client_ids.append(client_id)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"cet ID client {client_id} n'exite pas.")
        else:
            raise

print(f"Liste des ID valides : {valid_client_ids}")
"""

"""
client_id = 5
get_data_task = PythonOperator(
    task_id=f'get_data_{client_id}',
    python_callable=get_api_data,
    op_kwargs={'client_id': client_id},
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id=f'insert_data_{client_id}',
    python_callable=insert_data_to_postgres,
    op_kwargs={'client_id': client_id},
    dag=dag,
)

create_clients_table >> create_insights_table >> get_data_task >> insert_data_task
"""
