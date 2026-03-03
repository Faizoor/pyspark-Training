# PySpark 4.x Local Quickstart (Docker Compose)

Get a fully functional Apache Spark **4.1.1** cluster running locally in minutes using the official [apache/spark](https://hub.docker.com/r/apache/spark) Docker image.

## Cluster topology

### Spark cluster (`docker-compose.yml`)

| Service | Container | Port |
|---|---|---|
| Spark Master | `spark-master` | `7077` (cluster), `8080` (Web UI) |
| Spark Worker 1 | `spark-worker-1` | `8081` (Web UI) |
| Spark Worker 2 | `spark-worker-2` | `8082` (Web UI) |
| History Server | `spark-history` | `18080` (Web UI) |

### PostgreSQL stack (`docker-compose.postgres.yml`) — optional, for JDBC demo

| Service | Container | Port |
|---|---|---|
| PostgreSQL 16 | `spark-postgres` | `5432` |
| JDBC JAR downloader | `jdbc-jar-downloader` | — |

---

## Prerequisites

| Requirement | Minimum version | Check |
|---|---|---|
| **Docker Engine** | 24.x | `docker --version` |
| **Docker Compose** | v2.20 (plugin) | `docker compose version` |
| RAM | 6 GB available to Docker | Docker Desktop → Settings → Resources |
| Disk | 3 GB free (image download) | — |

> **Linux users** - Docker Engine and the Compose plugin are separate packages.  
> Install: `sudo apt install docker-ce docker-ce-cli containerd.io docker-compose-plugin`

---

## Quickstart

### 1. Clone / navigate to this directory

```bash
cd quickstart/
```

### 2. Pull images (optional — speeds up first start)

```bash
docker pull apache/spark:4.1.1-scala2.13-java21-python3-ubuntu
```

### 3. Start the cluster

```bash
docker compose up -d
```

Wait ~15 seconds for all services to be ready, then check status:

```bash
docker compose ps
```

Expected output — all services should be `running (healthy)` or `running`:

```
NAME              STATUS
spark-master      running (healthy)
spark-worker-1    running
spark-worker-2    running
spark-history     running
```

### 4. Open the Web UIs

| UI | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| Worker 1 | http://localhost:8081 |
| Worker 2 | http://localhost:8082 |
| History Server | http://localhost:18080 |

### 5. Submit the sample PySpark job

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/jobs/hello_spark.py
```

You should see word counts, a DataFrame, and SQL results printed to the terminal.

### 6. Interactive PySpark shell

```bash
docker exec -it spark-master /opt/spark/bin/pyspark \
  --master spark://spark-master:7077
```

### 7. Stop the cluster

```bash
# Stop and remove containers (keeps the spark-logs volume)
docker compose down

# Stop AND remove containers + volumes (including spark-logs)
docker compose down -v
```

---

## PostgreSQL JDBC demo (Section 14 of `spark_sql_demo.py`)

This optional stack adds a PostgreSQL 16 database on the same Docker network so
Spark can read from it via JDBC.

### 1. Start the Spark cluster first (if not already running)

```bash
docker compose up -d
```

### 2. Start PostgreSQL and download the JDBC JAR

```bash
docker compose -f docker-compose.postgres.yml up -d
```

This starts two one-time services:
- **`spark-postgres`** — PostgreSQL 16 pre-seeded with `public.employees` (same 5 rows as the JSON dataset).
- **`jdbc-jar-downloader`** — downloads `postgresql-42.7.3.jar` into `./jobs/` so every Spark container can reach it at `/opt/spark/jobs/postgresql-42.7.3.jar`.

Wait ~10 seconds for Postgres to be healthy:

```bash
docker inspect --format='{{.State.Health.Status}}' spark-postgres
# expected: healthy
```

> **If the JAR downloader fails** (permission error on the bind-mount), download it manually:  
> `curl -L -o jobs/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar`

### 3. Run the Spark SQL demo with the JDBC JAR

```bash
docker exec spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark/jobs/postgresql-42.7.3.jar \
    /opt/spark/jobs/spark_sql_demo.py
```

Section 14 of the output will show the employees table read live from PostgreSQL:

```
=================================================================
  SECTION 14 — JDBC read from PostgreSQL
=================================================================
→ Employees table read from PostgreSQL via JDBC:
+---+-----+-----------+----------+---------+
| id| name|  dept_name|dept_floor|   salary|
+---+-----+-----------+----------+---------+
|  1|Alice|Engineering|         3| 95000.00|
|  2|  Bob|  Marketing|         2| 72000.00|
|  3|Carol|Engineering|         3|105000.00|
|  4| Dave|         HR|         1| 65000.00|
|  5|  Eve|  Marketing|         2| 78000.00|
+---+-----+-----------+----------+---------+
```

### 4. Connect directly with psql (optional)

```bash
docker exec -it spark-postgres psql -U postgres -d mydb
```

### 5. Stop and remove both stacks

```bash
# Stop PostgreSQL stack (keeps postgres-data volume)
docker compose -f docker-compose.postgres.yml down

# Stop PostgreSQL stack AND remove its volume
docker compose -f docker-compose.postgres.yml down -v

# Stop Spark cluster (keeps spark-logs volume)
docker compose down

# Stop Spark cluster AND remove its volume
docker compose down -v

# Tear down everything in one go
docker compose -f docker-compose.postgres.yml down -v && docker compose down -v
```

---

## Project structure

```
quickstart/
├── docker-compose.yml          # Spark cluster (master, 2 workers, history server)
├── docker-compose.postgres.yml # Optional PostgreSQL stack for JDBC demo
├── README.md                   # This file
├── conf/
│   ├── spark-defaults.conf     # Spark configuration
│   └── spark-env.sh            # Worker resource settings
├── postgres/
│   └── init/
│       └── 01_employees.sql    # Seed data for the JDBC demo
└── jobs/
    ├── hello_spark.py          # Sample PySpark job
    ├── spark_sql_demo.py       # Spark SQL demo (Sections 01-16)
    └── postgresql-42.7.3.jar   # JDBC driver (downloaded by jdbc-jar-downloader)
```

Place your own `.py` job scripts under `jobs/` — the directory is mounted into every
container at `/opt/spark/jobs/`.

---

## Configuration

Key environment variables you can adjust in `docker-compose.yml`:

| Variable | Default | Description |
|---|---|---|
| `SPARK_WORKER_CORES` | `2` | CPU cores per worker |
| `SPARK_WORKER_MEMORY` | `2g` | Memory per worker |
| `SPARK_MASTER_PORT` | `7077` | Master RPC port |
| `SPARK_MASTER_WEBUI_PORT` | `8080` | Master Web UI port |

To scale workers dynamically:

```bash
docker compose up -d --scale spark-worker-1=3
```

> Note: when scaling, remove the explicit `container_name` from the worker services in
> `docker-compose.yml` first, as fixed names conflict with multiple replicas.

---

## Troubleshooting

**Workers not connecting to master**  
Verify the master is healthy: `docker compose ps spark-master`  
Check logs: `docker compose logs spark-master`

**Port already in use**  
Change the host-side port mapping in `docker-compose.yml`, e.g. `"9080:8080"`.

**Out of memory errors**  
Increase Docker's memory limit in Docker Desktop settings, or reduce
`SPARK_WORKER_MEMORY` in `docker-compose.yml`.

**Inspect container logs**

```bash
docker compose logs -f spark-master
docker compose logs -f spark-worker-1
```

---

## Image reference

- **Image**: `apache/spark:4.1.1-scala2.13-java21-python3-ubuntu`
- **Registry**: [hub.docker.com/r/apache/spark](https://hub.docker.com/r/apache/spark)
- **Spark version**: 4.1.1 (released Jan 09, 2026)
- **Java**: 21 | **Scala**: 2.13 | **Python**: 3.x
- **Supported platforms**: `linux/amd64`, `linux/arm64` (Apple Silicon)
