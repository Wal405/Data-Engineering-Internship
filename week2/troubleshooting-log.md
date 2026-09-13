# Week 2 – Airflow ELT Pipeline Troubleshooting

While working on the Airflow ELT pipeline, I ran into a few problems. I documented them below along with what caused them and how I fixed them.

## 1. `.env` file encoding error

When I tried to run Docker Compose, I got this error:

```text
failed to read .env: line 1: unexpected character "�" in variable name
```

My `.env` file contained:

```text
AIRFLOW_UID=50000
```

The problem was with the way I created the file in PowerShell. It was saved with the wrong encoding, which caused Docker Compose to read some extra characters from the file.

I recreated the file using:

```powershell
Set-Content -Path .env -Value "AIRFLOW_UID=50000" -Encoding ascii
```

After that, Docker Compose was able to read the file correctly.

**What I learned:** When creating `.env` or other configuration files on Windows, it is better to specify the encoding explicitly.

---

## 2. DAG was not found when I tried to trigger it

After creating my DAG, I tried to trigger it but got:

```text
airflow.exceptions.DagNotFound: Dag id elt_api_pipeline not found in DagModel
```

This was confusing because the DAG was showing when I ran:

```bash
airflow dags list
```

I first checked for DAG import errors:

```bash
airflow dags list-import-errors
```

There were no errors.

The problem was that Airflow had found the DAG file, but the scheduler had not completely updated its internal database with the new DAG yet.

I first tried:

```bash
airflow dags reserialize
```

but this caused another error because the scheduler was already doing its own synchronization.

Instead, I restarted the scheduler:

```powershell
docker compose restart airflow-scheduler
```

After the restart, the DAG was recognized properly and I was able to trigger it.

**What I learned:** If a newly created DAG is not available for triggering, restarting the scheduler can help it pick up the DAG properly.

---

## 3. DAG was triggered but tasks were not running

The DAG trigger command worked, but the run stayed in the `queued` state. The tasks were also not starting.

I checked the Docker containers first:

```bash
docker compose ps
```

All the containers were healthy.

Then I checked the worker logs. The worker was running and connected to Redis, so the worker was not the problem.

I also checked the scheduler logs, but it was not processing the DAG run.

Finally, I checked the DAG list and noticed that the DAG was paused:

```text
is_paused = True
```

That was the actual problem.

I unpaused it using:

```powershell
docker exec -it airflow-elt-pipeline-airflow-scheduler-1 airflow dags unpause elt_api_pipeline
```

After unpausing and triggering the DAG again, the tasks ran successfully.

**What I learned:** If a DAG is triggered but the tasks are not running, check whether the DAG is paused.

---

## 4. Duplicate rows were being added

After running the pipeline twice, I noticed that `raw_products` contained 40 rows instead of 20.

The API was returning 20 products, but every time I ran the pipeline, those 20 products were inserted again.

The original query was using a normal `INSERT`, so there was nothing stopping the same product from being inserted multiple times.

I fixed this by making `productid` the primary key and using `ON CONFLICT`:

```sql
INSERT INTO raw_products (productid, title, price, category)
VALUES (%s, %s, %s, %s)
ON CONFLICT (productid) DO UPDATE SET
    title = EXCLUDED.title,
    price = EXCLUDED.price,
    category = EXCLUDED.category;
```

Now, if the product already exists, its information is updated instead of inserting another row.

I tested it by running the pipeline twice and checking the table. It still had 20 rows.

**What I learned:** A pipeline can run multiple times because of retries or scheduled runs, so the loading process should be designed to avoid duplicate data.

---

## 5. `next_dagrun` showed an unexpected date

After adding a daily schedule using:

```python
@daily
```

I checked the DAG details:

```bash
airflow dags details elt_api_pipeline
```

The `next_dagrun` date was not what I expected based on my `start_date` and `catchup=False` settings.

I did not fully investigate this because it was not affecting the actual pipeline. The extraction and loading were already working correctly when I triggered the DAG manually.

I noted it for further investigation. If this happened in a real production pipeline, I would look further into Airflow's scheduling and data interval behavior.

---

# Commands I Used for Troubleshooting

| Command                                              | What I used it for                                |
| ---------------------------------------------------- | ------------------------------------------------- |
| `airflow dags list-import-errors`                    | Check if there was a problem loading the DAG      |
| `airflow dags list`                                  | Check available DAGs and whether they were paused |
| `airflow dags details <dag_id>`                      | Check the configuration and status of a DAG       |
| `airflow dags trigger <dag_id>`                      | Manually run a DAG                                |
| `airflow dags list-runs -d <dag_id>`                 | Check previous DAG runs                           |
| `airflow tasks states-for-dag-run <dag_id> <run_id>` | Check individual task states                      |
| `docker compose ps`                                  | Check whether Docker containers were running      |
| `docker logs <container_name> --tail 50`             | Check recent logs from a container                |

## Final Notes

These issues helped me understand that when an Airflow pipeline does not work, I should not immediately assume that the code is wrong. I need to check the different parts one by one, such as Docker containers, the scheduler, worker, DAG status, task status, and finally the data in the database.
