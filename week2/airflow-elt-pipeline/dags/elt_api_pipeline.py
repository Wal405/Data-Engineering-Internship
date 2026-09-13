from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests

def extract_products(**context):
    """Call FakeStore API and push raw JSON to XCom for the next task."""
    response = requests.get("https://fakestoreapi.com/products")
    response.raise_for_status()
    products = response.json()
    context['ti'].xcom_push(key='products', value=products)
    print(f"Extracted {len(products)} products")

def load_raw(**context):
    """Load raw JSON into a raw table — idempotent, safe to re-run."""
    products = context['ti'].xcom_pull(key='products', task_ids='extract_products')
    hook = PostgresHook(postgres_conn_id='postgres_week2')

    hook.run("""
        CREATE TABLE IF NOT EXISTS raw_products (
            productid INT PRIMARY KEY,
            title TEXT,
            price NUMERIC,
            category TEXT
        );
    """)

    for p in products:
        hook.run(
            """
            INSERT INTO raw_products (productid, title, price, category)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (productid) DO UPDATE SET
                title = EXCLUDED.title,
                price = EXCLUDED.price,
                category = EXCLUDED.category
            """,
            parameters=(p['id'], p['title'], p['price'], p['category'])
        )
    print(f"Upserted {len(products)} rows into raw_products")

with DAG(
    dag_id='elt_api_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule="@daily",  
) as dag:

    extract_task = PythonOperator(
        task_id='extract_products',
        python_callable=extract_products,
    )

    load_task = PythonOperator(
        task_id='load_raw',
        python_callable=load_raw,
    )

    extract_task >> load_task