# Week 2 – Airflow ELT Pipeline

This project is a simple ELT pipeline built using Apache Airflow and Docker. It gets product data from the FakeStore API and loads it into a PostgreSQL database.

The pipeline is designed so that running it multiple times does not create duplicate products.

## Architecture

```text
FakeStore API
     ↓
extract_products
     ↓
XCom
     ↓
load_raw
     ↓
PostgreSQL
     ↓
raw_products
```

The pipeline is scheduled to run once every day using Airflow's `@daily` schedule.

Airflow's scheduler manages the tasks, while the worker executes them.

## Prerequisites

Before running the project, make sure you have:

* Docker Desktop installed and running
* PostgreSQL installed on your local machine
* A PostgreSQL database created for the pipeline

## Setup

### 1. Create the PostgreSQL database

Create the database in your local PostgreSQL installation:

```sql
CREATE DATABASE week2_elt_db;
```

The database is running on the local machine, not inside the Airflow Docker containers.

### 2. Create the `.env` file

Create a `.env` file inside the project folder:

```text
AIRFLOW_UID=50000
```

On Windows, I used the following command to avoid an encoding problem:

```powershell
Set-Content -Path .env -Value "AIRFLOW_UID=50000" -Encoding ascii
```

### 3. Initialize Airflow

This only needs to be done the first time:

```powershell
docker compose up airflow-init
```

### 4. Start the Airflow services

```powershell
docker compose up -d
```

### 5. Check the containers

```powershell
docker compose ps
```

The Airflow stack should show the required services running, including:

* Webserver
* Scheduler
* Worker
* Triggerer
* PostgreSQL
* Redis

### 6. Open Airflow

Open:

```text
http://localhost:8080
```

Login:

```text
Username: airflow
Password: airflow
```

### 7. Add the PostgreSQL connection

In Airflow, go to:

**Admin → Connections → +**

Use the following settings:

```text
Connection Id: postgres_week2
Connection Type: Postgres
Host: host.docker.internal
Schema: week2_elt_db
Login: postgres
Password: your PostgreSQL password
Port: 5432
```

`host.docker.internal` is used because PostgreSQL is running on my Windows machine while Airflow is running inside Docker.

### 8. Unpause the DAG

New DAGs can be paused by default, so I unpaused the DAG using:

```powershell
docker exec -it airflow-elt-pipeline-airflow-scheduler-1 airflow dags unpause elt_api_pipeline
```

## Running the Pipeline

The DAG can be triggered manually from the command line:

```powershell
docker exec -it airflow-elt-pipeline-airflow-scheduler-1 airflow dags trigger elt_api_pipeline
```

It can also be triggered from the Airflow UI by finding `elt_api_pipeline` and clicking the play button.

Once unpaused, the DAG is also scheduled to run automatically every day.

## Checking the Results

After the pipeline runs, I can check the number of products in PostgreSQL:

```powershell
psql -U postgres -d week2_elt_db -c "SELECT COUNT(*) FROM raw_products;"
```

The expected result is:

```text
20
```

Running the pipeline multiple times should still leave 20 rows because the pipeline uses an UPSERT instead of blindly inserting duplicate records.

## Key Design Decisions

### `host.docker.internal`

I used:

```text
host.docker.internal
```

instead of:

```text
localhost
```

because PostgreSQL is running on my Windows machine, while Airflow is running inside Docker.

`localhost` inside an Airflow container refers to that container itself, not my Windows machine.

### Idempotent UPSERT

The pipeline uses:

```sql
ON CONFLICT (...) DO UPDATE
```

This prevents duplicate products when the pipeline runs again.

For example, if product `1` already exists, another run will update that product instead of inserting another row.

This is useful because Airflow pipelines can run multiple times due to scheduled runs, retries, or manual triggers.

### XCom

The `extract_products` task gets the data from the API and passes it to the `load_raw` task using Airflow XCom.

```text
extract_products
       ↓
      XCom
       ↓
   load_raw
```

This keeps the extraction and loading logic separated into two tasks.

### Daily Schedule

The DAG uses:

```python
schedule="@daily"
catchup=False
```

This makes the pipeline run once per day without creating old runs for every date between the `start_date` and today.

## Project Files

| File                       | Purpose                                                               |
| -------------------------- | --------------------------------------------------------------------- |
| `docker-compose.yaml`      | Sets up the Airflow services using Docker                             |
| `dags/elt_api_pipeline.py` | Contains the Airflow DAG and pipeline logic                           |
| `troubleshooting-log.md`   | Contains the problems I faced during development and how I fixed them |

## Troubleshooting

During development, I faced issues with the `.env` file encoding, DAG synchronization, paused DAGs, and duplicate rows.

I documented these problems and their solutions in:

```text
troubleshooting-log.md
```

This helped me understand how to troubleshoot Airflow instead of only focusing on getting the pipeline to run.
