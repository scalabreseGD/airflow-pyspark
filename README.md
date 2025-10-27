# Data Lake with Bronze/Silver/Gold Architecture

A production-ready data lake implementation using Apache Spark, Apache Airflow, Hive Metastore, and MinIO, following the Medallion Architecture (Bronze/Silver/Gold layers).

## Architecture Overview

This project implements a modern data lakehouse architecture with three layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Data Lake Architecture                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Source Data (CSV) → Bronze (Raw) → Silver (Cleaned) → Gold     │
│                                                           (Aggregated)
│                                                                   │
│  - CSV files         - Exact copy      - Validated    - Business │
│  - Multiple systems  - All columns     - Cleaned      - Aggregated│
│  - Raw format        - Partitioned     - Typed        - Ready for │
│                      - Audit trail     - Transformed  - Analytics │
└─────────────────────────────────────────────────────────────────┘

Technology Stack:
┌─────────────────┐
│ Apache Airflow  │ ←→ Orchestrates ETL workflows
│   + Scheduler   │
└────────┬────────┘
         │
         ├─────────→ Apache Spark (ETL Processing)
         │           • Master + Worker nodes
         │           • PySpark for transformations
         │
         ├─────────→ MinIO (S3-compatible storage)
         │           • s3a://warehouse/ (Hive tables)
         │           • s3a://data/ (Source data & staging)
         │
         └─────────→ Hive Metastore
                     └─→ PostgreSQL (Metadata DB)
```

## Features

- **Medallion Architecture**: Bronze (raw), Silver (cleaned), Gold (aggregated) layers
- **Apache Airflow 2.7.0**: Workflow orchestration with DAGs
- **Apache Spark 3.5.3**: Distributed data processing
- **Hive Metastore 3.1.3**: Centralized table metadata management
- **MinIO**: S3-compatible object storage for data lake
- **PostgreSQL**: Backend for Hive Metastore and Airflow
- **Jupyter Notebooks**: Interactive development and table creation
- **Full S3A protocol support**: Direct reads/writes to MinIO
- **Partitioned tables**: Optimized for performance and cost
- **Knowledge Graph**: Automatic DAG visualization and lineage tracking with Memgraph

## Quick Start

### 1. Start All Services

```bash
./start.sh
```

This will:
- Build custom Docker images (Spark + Airflow)
- Start all services with health checks
- Display service URLs

### 2. Set Up Airflow Connections

```bash
./setup_airflow_connections.sh
```

This creates the `spark_default` connection in Airflow to submit jobs to the Spark cluster.

### 3. Create Tables

Open Jupyter and run the notebooks in order:

1. **Create Bronze Tables**: `notebooks/create_bronze_tables.ipynb`
2. **Create Silver Tables**: `notebooks/create_silver_tables.ipynb`
3. **Create Gold Tables**: `notebooks/create_gold_tables.ipynb`

### 4. Run Bronze Data Ingestion

#### Option A: Via Airflow (Recommended)

1. Open Airflow UI: http://localhost:8082 (admin / admin)
2. Enable the `ingest_bronze_data` DAG
3. Trigger the DAG manually

#### Option B: Direct Submission

```bash
./submit.sh ingest_bronze_data.py
```

### 5. Verify Data

- **MinIO Console**: http://localhost:9001 (admin / admin123)
- **Spark Master UI**: http://localhost:8080
- **Airflow UI**: http://localhost:8082 (admin / admin)

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8082 | admin / admin |
| Spark Master UI | http://localhost:8080 | - |
| Spark Worker UI | http://localhost:8081 | - |
| Spark Job UI | http://localhost:4040 | (When job is running) |
| Jupyter Notebook | http://localhost:8888 | Token in logs |
| MinIO Console | http://localhost:9001 | admin / admin123 |
| MinIO API | http://localhost:9000 | admin / admin123 |
| Hive Metastore | thrift://localhost:9083 | - |
| PostgreSQL | localhost:5432 | hive / hive123 |
| Memgraph Lab | http://localhost:3000 | - (if running) |

## Data Layer Structure

### Bronze Layer (Raw Data)

**Database**: `bronze`

The bronze layer stores raw data exactly as received from source systems with audit columns:

- `_source_system`: Origin of the data
- `_ingestion_timestamp`: When the data was ingested
- `_file_name`: Source file name
- `_record_offset`: Position in source file

**Tables**:
- `transactions_raw`
- `transaction_items_raw`
- `subscriptions_raw`
- `customer_interactions_raw`
- `product_catalog_raw`
- `inventory_snapshots_raw`
- `marketing_campaigns_raw`
- `campaign_events_raw`

**Location**: `s3a://warehouse/bronze.db/`

### Silver Layer (Cleaned & Validated)

**Database**: `silver`

The silver layer contains cleaned, validated, and typed data ready for analytics:

- Data types properly cast
- Invalid records filtered or corrected
- Duplicates removed
- Business rules applied
- Still detailed, not aggregated

**Tables**:
- `transactions`
- `transaction_items`
- `subscriptions`
- `customer_interactions`
- `product_catalog`
- `inventory_snapshots`
- `marketing_campaigns`
- `campaign_events`

**Location**: `s3a://warehouse/silver.db/`

### Gold Layer (Business-Level Aggregates)

**Database**: `gold`

The gold layer contains business-level aggregates optimized for reporting and analytics:

- Pre-aggregated metrics
- Denormalized for query performance
- Business KPIs and metrics
- Ready for BI tools

**Tables**:
- `product_performance` - Product sales performance and profitability metrics
- `customer_360` - Comprehensive customer profiles with all touchpoints
- `store_performance` - Store-level performance metrics
- `subscription_health` - Subscription metrics and health indicators
- `basket_analysis` - Market basket analysis and product associations
- `campaign_roi_analysis` - Campaign performance and ROI metrics
- `category_brand_performance` - Category and brand-level performance
- `channel_attribution` - Multi-channel attribution analysis
- `cohort_analysis` - Cohort-based customer analysis

**Location**: `s3a://warehouse/gold.db/`

## Project Structure

```
.
├── docker-compose.yml              # Services orchestration
├── docker/
│   ├── spark/Dockerfile            # Spark with S3A JARs
│   ├── hive/Dockerfile             # Hive Metastore with Hadoop 3.3.4
│   └── jupyter/Dockerfile          # Jupyter with PySpark
├── conf/
│   ├── hive/                       # Hive Metastore configuration
│   └── spark/                      # Spark configuration
├── dags/
│   ├── ingest_bronze_data_dag.py         # Bronze layer ingestion DAG
│   ├── bronze_to_silver_pipeline_dag.py  # Bronze → Silver transformation DAG
│   └── silver_to_gold_pipeline_dag.py    # Silver → Gold analytics DAG
├── spark-apps/
│   ├── ingest_bronze_data.py             # Bronze layer ingestion
│   ├── bronze_to_silver_*.py             # Silver layer transformations (6 jobs)
│   ├── gold_*.py                         # Gold layer analytics (9 jobs)
│   └── validate_bronze_data.py           # Bronze data validation
├── notebooks/
│   ├── create_bronze_tables.ipynb  # Create bronze layer tables
│   ├── create_silver_tables.ipynb  # Create silver layer tables
│   ├── create_gold_tables.ipynb    # Create gold layer tables
│   └── spark_cluster_demo.ipynb    # Demo notebook
├── source_data/                    # Source CSV files
├── start.sh                        # Startup script
├── submit.sh                       # Job submission script
├── setup_airflow_connections.sh    # Airflow setup script
├── create-kb/
│   ├── parse-airflow.py            # DAG knowledge graph generator
│   └── README.md                   # Knowledge graph documentation
└── plugins/
    └── dag_code.py                 # Custom Airflow plugin for DAG code extraction
```

## DAG Knowledge Graph

The project includes automated knowledge graph generation that extracts DAG metadata and loads it into Memgraph for visualization and analysis.

### Features

- **Automatic DAG Discovery**: Extracts all DAGs, tasks, and dependencies from Airflow
- **AST Parsing**: Analyzes DAG Python files to understand task relationships
- **Graph Database**: Stores pipeline metadata in Memgraph for querying and visualization
- **Spark Job Tracking**: Identifies and tracks all Spark jobs in your pipeline
- **Real-time Updates**: Refresh the graph anytime to reflect current pipeline state
- **Custom Airflow Plugin**: Extends Airflow REST API to expose DAG source code

### Quick Start

**Note**: The custom Airflow plugin (`plugins/dag_code.py`) is automatically loaded when you start Airflow with this project.

1. **Install Dependencies**:
   ```bash
   pip install neo4j requests
   ```

2. **Verify Plugin is Loaded**:
   ```bash
   curl http://localhost:8082/api/v1/dag_code/health
   # Should return: {"status": "healthy", ...}
   ```

3. **Start Memgraph** (optional - for visualization):
   ```bash
   docker run -d --name memgraph -p 7687:7687 -p 3000:3000 memgraph/memgraph-platform
   ```

4. **Generate Knowledge Graph**:
   ```bash
   python create-kb/parse-airflow.py
   ```

5. **Visualize** (open Memgraph Lab):
   ```
   http://localhost:3000
   ```

   Example query:
   ```cypher
   MATCH (d:DAG)-[:CONTAINS]->(t:Task)-[:DEPENDS_ON]->(upstream)
   RETURN d, t, upstream;
   ```

### What It Captures

- DAG nodes with task counts and Spark job metrics
- Task nodes with operator types, parameters, and Spark applications
- Dependency relationships (>>, <<, set_upstream/downstream)
- Trigger relationships (TriggerDagRunOperator)
- Complete data lineage through bronze → silver → gold

See [create-kb/README.md](create-kb/README.md) for detailed documentation, examples, and query recipes.

## Development Workflow

### 1. Interactive Development (Jupyter)

Access Jupyter for interactive development:

```bash
# Get Jupyter token from logs
docker-compose logs jupyter | grep token

# Open in browser: http://localhost:8888
```

Use notebooks to:
- Create and modify table schemas
- Test data transformations
- Explore data interactively
- Debug issues

### 2. Production Jobs (Spark)

Write production Spark jobs in `spark-apps/`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyETLJob") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    # Use Hive tables
    spark.sql("USE bronze")
    df = spark.table("transactions_raw")

    # Transform data
    transformed = df.filter(df.status == "completed")

    # Write to silver layer
    transformed.write \
        .mode("overwrite") \
        .insertInto("silver.transactions")

finally:
    spark.stop()
```

Submit jobs:

```bash
./submit.sh my_etl_job.py
```

### 3. Orchestration (Airflow)

Create DAGs in `dags/` to orchestrate workflows:

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    'silver_layer_etl',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
) as dag:

    process_silver = SparkSubmitOperator(
        task_id='process_silver',
        application='/opt/spark-apps/process_silver.py',
        conn_id='spark_default',
        conf={
            'spark.sql.hive.metastore.uris': 'thrift://hive-metastore:9083',
            # ... other Spark configs
        },
    )
```

## Managing the Stack

### Start Services

```bash
./start.sh
```

### Stop Services

```bash
docker-compose down
```

### Clean Start (Remove all data)

```bash
./start.sh --clean
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f spark-master
docker-compose logs -f airflow-scheduler
docker-compose logs -f hive-metastore
```

### Check Service Status

```bash
docker-compose ps
```

### Access Jupyter Token

```bash
docker-compose logs jupyter | grep token
```

## Writing Spark Jobs

### Basic Template

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    # Your ETL logic here
    spark.sql("USE bronze")
    df = spark.table("transactions_raw")

    # Process data
    result = df.filter(df.status == "active")

    # Write to silver
    result.write.mode("overwrite").insertInto("silver.transactions")

finally:
    spark.stop()
```

### Reading from Bronze

```python
# Option 1: Using table names (recommended)
spark.sql("USE bronze")
df = spark.table("transactions_raw")

# Option 2: Using SQL
df = spark.sql("SELECT * FROM bronze.transactions_raw WHERE _ingestion_timestamp > '2024-01-01'")
```

### Writing to Silver/Gold

```python
# Clean and transform
cleaned_df = bronze_df.dropDuplicates() \
    .filter(col("amount") > 0) \
    .withColumn("amount", col("amount").cast("double"))

# Write to silver
cleaned_df.write \
    .mode("overwrite") \
    .insertInto("silver.transactions")
```

## Troubleshooting

### Services Not Starting

Check logs:
```bash
docker-compose logs <service-name>
```

Verify all services are healthy:
```bash
docker-compose ps
```

### Airflow DAG Not Appearing

- Check scheduler logs: `docker-compose logs airflow-scheduler`
- Verify DAG syntax: `docker-compose exec airflow-scheduler airflow dags list`

### Spark Job Failing

- Check Spark Master UI: http://localhost:8080
- View job logs: `docker-compose logs spark-master`
- Check Hive Metastore connection: `docker-compose logs hive-metastore`

### Cannot Find Tables

Verify tables exist:
```bash
docker exec -it spark-master /opt/spark/bin/pyspark

# In PySpark:
spark.sql("SHOW DATABASES").show()
spark.sql("USE bronze")
spark.sql("SHOW TABLES").show()
```

### MinIO Connection Issues

- Verify MinIO is running: http://localhost:9001
- Check credentials in configuration files
- Ensure buckets exist (created automatically)

## Running the Complete Pipeline

### Bronze to Silver Transformation

1. Open Airflow UI: http://localhost:8082 (admin / admin)
2. Enable the `bronze_to_silver_pipeline` DAG
3. Trigger the DAG manually

This will:
- Trigger bronze data ingestion
- Validate bronze data
- Transform all 6 tables to silver layer (parallel execution with concurrency 3)

### Silver to Gold Analytics

1. Open Airflow UI: http://localhost:8082
2. Enable the `silver_to_gold_pipeline` DAG
3. Trigger the DAG manually

This will:
- Generate 9 gold layer analytics tables (parallel execution with concurrency 3)
- Product performance, customer 360, store metrics, and more

## Next Steps

1. **Schedule DAGs**: Set up daily/hourly schedules for automated pipelines
2. **Add Data Quality Checks**: Implement additional validation rules between layers
3. **Set Up Monitoring**: Configure alerts for failed jobs
4. **Connect BI Tools**: Integrate with Tableau, PowerBI, etc. to query gold tables
5. **Optimize Performance**: Tune Spark configurations for your workload
6. **Add Data Governance**: Implement data lineage and quality metrics
7. **Explore Knowledge Graph**: Use `create-kb/parse-airflow.py` to visualize pipeline dependencies and track Spark jobs

## Best Practices

- **Bronze Layer**: Keep raw data immutable, never modify
- **Silver Layer**: Apply business rules, clean data, maintain detail
- **Gold Layer**: Pre-aggregate for performance, denormalize as needed
- **Partitioning**: Use date-based partitions for large tables
- **Idempotency**: Ensure jobs can be re-run safely
- **Testing**: Test transformations in Jupyter before deploying
- **Monitoring**: Monitor Airflow and Spark UIs regularly

## Support and Documentation

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Hive Documentation](https://hive.apache.org/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

## License

This project is provided as-is for educational and development purposes.