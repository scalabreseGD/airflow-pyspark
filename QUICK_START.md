# Quick Start Guide - Bronze/Silver/Gold Data Lake

Get up and running with the Bronze/Silver/Gold data lakehouse in 5 minutes.

## What Is This?

A complete, production-ready data lakehouse implementing the **Medallion Architecture**:

- **Bronze Layer**: Raw data exactly as received from source systems
- **Silver Layer**: Cleaned, validated, and typed data
- **Gold Layer**: Business-level aggregates ready for analytics

## Technology Stack

- **Apache Spark 3.5.3**: Data processing and transformations
- **Apache Airflow 2.7.0**: Workflow orchestration
- **Hive Metastore 3.1.3**: Table metadata management
- **MinIO**: S3-compatible object storage
- **Jupyter**: Interactive development
- **PostgreSQL**: Backend for metadata
- **Neo4j**: Data lineage and knowledge graph

## Getting Started

### Step 1: Start All Services

```bash
./start.sh
```

**What happens:**
- Builds Docker images (first time takes ~5 minutes)
- Starts Spark, Airflow, Hive, MinIO, Jupyter, PostgreSQL
- Waits for all services to be healthy
- Displays service URLs

**Service URLs:**
- Airflow: http://localhost:8082 (admin / admin)
- Jupyter: http://localhost:8888 (token in logs)
- Spark Master: http://localhost:8080
- MinIO Console: http://localhost:9001 (admin / admin123)
- Neo4j Browser: http://localhost:7474 (neo4j / neo4j123)

### Step 2: Configure Airflow

```bash
./setup_airflow_connections.sh
```

**What happens:**
- Creates `spark_default` connection in Airflow
- Configures Spark Master endpoint
- Enables Airflow to submit Spark jobs

### Step 3: Create Table Schemas

Open Jupyter Notebook to create the bronze/silver/gold table schemas:

```bash
# Get Jupyter token
docker-compose logs jupyter | grep token

# Open: http://localhost:8888
```

**Run these notebooks in order:**

1. **`notebooks/create_bronze_tables.ipynb`**
   - Creates `bronze` database
   - Creates 8 raw data tables with audit columns
   - Tables: transactions_raw, transaction_items_raw, subscriptions_raw, etc.

2. **`notebooks/create_silver_tables.ipynb`**
   - Creates `silver` database
   - Creates cleaned and typed versions of bronze tables
   - Removes audit columns, adds business logic

3. **`notebooks/create_gold_tables.ipynb`**
   - Creates `gold` database
   - Creates aggregated business metrics tables
   - Tables: customer_summary, product_performance, sales_metrics, etc.

### Step 4: Ingest Bronze Data

Now that tables exist, ingest the CSV data into bronze tables.

#### Option A: Via Airflow UI (Recommended)

1. Open Airflow: http://localhost:8082 (admin / admin)
2. Find the `ingest_bronze_data` DAG
3. Toggle it ON (unpause)
4. Click "Trigger DAG" (play button)
5. Monitor progress in the Airflow UI

#### Option B: Direct Spark Submit

```bash
./submit.sh ingest_bronze_data.py
```

**What happens:**
- Reads CSV files from `s3a://data/source_data/`
- Validates and transforms data
- Writes to bronze tables in Parquet format
- Partitions by ingestion timestamp

### Step 5: Verify Everything Works

#### Check Airflow

```bash
# Open Airflow UI
http://localhost:8082

# View the DAG run
# All tasks should be green (success)
```

#### Check MinIO

```bash
# Open MinIO Console
http://localhost:9001 (admin / admin123)

# Browse to: warehouse/bronze.db/
# You should see folders for each table with Parquet files
```

#### Query Data in Jupyter

Open a Jupyter notebook and run:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyData") \
    .enableHiveSupport() \
    .getOrCreate()

# Check bronze data
spark.sql("USE bronze")
spark.sql("SHOW TABLES").show()

# Query a table
df = spark.table("transactions_raw")
print(f"Total records: {df.count()}")
df.show(5)

# Check row counts
spark.sql("""
    SELECT
        'transactions_raw' as table_name,
        COUNT(*) as row_count
    FROM bronze.transactions_raw
""").show()
```

## What's Next?

You now have raw data in the bronze layer. Next steps:

### 1. Run Bronze to Silver Pipeline

The silver layer transformations are already implemented. Run them via Airflow:

**Option A: Via Airflow UI (Recommended)**
1. Open Airflow: http://localhost:8082 (admin / admin)
2. Find the `bronze_to_silver_pipeline` DAG
3. Toggle it ON (unpause)
4. Click "Trigger DAG" (play button)

**What happens:**
- Triggers bronze data ingestion
- Validates bronze data
- Transforms 6 tables to silver layer in parallel (concurrency: 3)
- Tables: transactions, transaction_items, subscriptions, product_catalog, inventory_snapshots, customer_interactions

### 2. Run Silver to Gold Pipeline

The gold layer analytics are already implemented. Run them via Airflow:

**Option A: Via Airflow UI (Recommended)**
1. Open Airflow: http://localhost:8082 (admin / admin)
2. Find the `silver_to_gold_pipeline` DAG
3. Toggle it ON (unpause)
4. Click "Trigger DAG" (play button)

**What happens:**
- Generates 9 gold analytics tables in parallel (concurrency: 3)
- Tables: product_performance, customer_360, store_performance, subscription_health, basket_analysis, campaign_roi_analysis, category_brand_performance, channel_attribution, cohort_analysis

### 3. (Optional) Create Custom Silver Layer Transformations

If you want to add custom transformations, create: `spark-apps/custom_silver.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim

spark = SparkSession.builder \
    .appName("ProcessSilverLayer") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    # Read from bronze
    spark.sql("USE bronze")
    bronze_df = spark.table("transactions_raw")

    # Clean and transform
    silver_df = bronze_df \
        .dropDuplicates() \
        .filter(col("transaction_amount") > 0) \
        .withColumn("transaction_amount", col("transaction_amount").cast("double")) \
        .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")) \
        .withColumn("customer_name", trim(col("customer_name"))) \
        .drop("_source_system", "_ingestion_timestamp", "_file_name", "_record_offset")

    # Write to silver
    silver_df.write \
        .mode("overwrite") \
        .insertInto("silver.transactions")

    print(f"Processed {silver_df.count()} records to silver layer")

finally:
    spark.stop()
```

Submit it:
```bash
./submit.sh custom_silver.py
```

### 4. Schedule Your Pipelines

To run the pipelines automatically, edit the DAGs:

**For `bronze_to_silver_pipeline_dag.py`:**
```python
schedule_interval='@daily',  # Runs at midnight daily
# or
schedule_interval='0 2 * * *',  # Runs at 2 AM daily
```

**For `silver_to_gold_pipeline_dag.py`:**
```python
schedule_interval='0 3 * * *',  # Runs at 3 AM daily (after silver completes)
```

Or chain them together with `TriggerDagRunOperator` for automatic flow: bronze → silver → gold.

## Common Commands

### Managing Services

```bash
# Start all services
./start.sh

# Stop all services
docker-compose down

# Clean restart (removes all data)
./start.sh --clean

# View logs
docker-compose logs -f spark-master
docker-compose logs -f airflow-scheduler

# Check status
docker-compose ps
```

### Submitting Spark Jobs

```bash
# Submit a job
./submit.sh ingest_bronze_data.py

# Submit with logs
./submit.sh ingest_bronze_data.py 2>&1 | tee job.log
```

### Accessing Services

```bash
# Get Jupyter token
docker-compose logs jupyter | grep token

# Access PySpark shell
docker exec -it spark-master /opt/spark/bin/pyspark

# Access Airflow CLI
docker-compose exec airflow-scheduler airflow dags list

# Check Hive tables
docker exec -it spark-master /opt/spark/bin/pyspark
>>> spark.sql("SHOW DATABASES").show()
>>> spark.sql("USE bronze")
>>> spark.sql("SHOW TABLES").show()
```

### Querying Data

```bash
# List MinIO data
docker exec minio-setup mc ls myminio/warehouse/ --recursive

# Query in PySpark
docker exec -it spark-master /opt/spark/bin/pyspark
>>> df = spark.table("bronze.transactions_raw")
>>> df.show()
```

## Project Structure

```
.
├── dags/
│   ├── ingest_bronze_data_dag.py         # Bronze ingestion DAG
│   ├── bronze_to_silver_pipeline_dag.py  # Bronze → Silver pipeline
│   └── silver_to_gold_pipeline_dag.py    # Silver → Gold pipeline
├── spark-apps/
│   ├── ingest_bronze_data.py             # Bronze ingestion
│   ├── bronze_to_silver_*.py             # Silver transformations (6 jobs)
│   ├── gold_*.py                         # Gold analytics (9 jobs)
│   └── validate_bronze_data.py           # Data validation
├── notebooks/
│   ├── create_bronze_tables.ipynb        # Create bronze schemas
│   ├── create_silver_tables.ipynb        # Create silver schemas
│   └── create_gold_tables.ipynb          # Create gold schemas
├── source_data/                          # CSV source files
├── start.sh                              # Start services
├── submit.sh                             # Submit Spark jobs
└── setup_airflow_connections.sh          # Configure Airflow
```

## Data Flow

```
Source CSV Files (source_data/)
        ↓
    [Airflow DAG]
        ↓
Bronze Layer (bronze.db/)
    - Raw data with audit columns
    - Partitioned by ingestion timestamp
    - Immutable (never modify)
        ↓
    [Your ETL Job]
        ↓
Silver Layer (silver.db/)
    - Cleaned and validated
    - Proper data types
    - Business rules applied
    - Duplicates removed
        ↓
    [Your Aggregation Job]
        ↓
Gold Layer (gold.db/)
    - Pre-aggregated metrics
    - Denormalized for performance
    - Ready for BI tools
    - Optimized for queries
```

## Troubleshooting

### Services won't start

```bash
# Check what's failing
docker-compose ps

# View logs
docker-compose logs <service-name>

# Common fixes
docker-compose down
docker system prune -f
./start.sh --clean
```

### DAG not appearing in Airflow

```bash
# Check scheduler logs
docker-compose logs airflow-scheduler

# Verify DAG syntax
docker-compose exec airflow-scheduler airflow dags list

# Restart scheduler
docker-compose restart airflow-scheduler
```

### Tables not found

```bash
# Verify tables exist
docker exec -it spark-master /opt/spark/bin/pyspark

# In PySpark:
spark.sql("SHOW DATABASES").show()
spark.sql("USE bronze")
spark.sql("SHOW TABLES").show()

# If empty, run the create_bronze_tables.ipynb notebook again
```

### Jupyter token not working

```bash
# Get a fresh token
docker-compose logs jupyter | grep token

# Or restart Jupyter
docker-compose restart jupyter
```

## Key Concepts

### Bronze Layer
- **Purpose**: Store raw data exactly as received
- **When to use**: Always ingest raw data first
- **Never**: Modify or delete bronze data

### Silver Layer
- **Purpose**: Clean, validate, and standardize data
- **When to use**: For analytics on detailed records
- **Apply**: Data quality rules, type conversions, deduplication

### Gold Layer
- **Purpose**: Business-level aggregates and metrics
- **When to use**: For dashboards, reports, and BI tools
- **Contains**: KPIs, summaries, pre-joined datasets

## Best Practices

1. **Always start with Bronze**: Ingest raw data first, transform later
2. **Make jobs idempotent**: Jobs should produce same results when re-run
3. **Test in Jupyter first**: Validate transformations before deploying
4. **Monitor Airflow**: Check DAG runs daily
5. **Partition large tables**: Use date-based partitions for performance
6. **Use Hive tables**: Manage metadata through Hive Metastore
7. **Document transformations**: Add comments to Spark jobs

## Viewing Data Lineage (Optional but Recommended)

All Spark jobs automatically track data lineage and push it to Neo4j. This lets you visualize how data flows through your pipeline.

### Start Neo4j

```bash
docker-compose up -d neo4j
```

### View Lineage

1. Open **Neo4j Browser**: http://localhost:7474
2. Run queries to explore your data flows:

**See all data flows:**
```cypher
MATCH (src:Dataset)-[:FLOWS_TO]->(job:SparkJob)-[:WRITES_TO]->(dst:Dataset)
RETURN src, job, dst;
```

**Complete pipeline view (Bronze → Silver → Gold):**
```cypher
MATCH path = (bronze:Dataset)-[:FLOWS_TO]->(j1:SparkJob)-[:WRITES_TO]->
             (silver:Dataset)-[:FLOWS_TO]->(j2:SparkJob)-[:WRITES_TO]->(gold:Dataset)
WHERE bronze.name STARTS WITH "bronze." 
  AND silver.name STARTS WITH "silver."
  AND gold.name STARTS WITH "gold."
RETURN path
LIMIT 20;
```

**Find what depends on a table (impact analysis):**
```cypher
MATCH path = (source:Dataset {name: "bronze.transactions_raw"})
      -[:FLOWS_TO*1..10]->(downstream)
RETURN DISTINCT downstream.name AS affected_tables;
```

### How It Works

Every Spark job includes this code after creating the SparkSession:

```python
from lineage_listener import register_lineage_listener
register_lineage_listener(spark)
```

This automatically tracks:
- All tables and files read by the job
- All tables and files written by the job
- Creates a graph showing complete data flows

**Configuration:**
Lineage tracking is enabled by default. To disable it, set:
```bash
export LINEAGE_TO_NEO4J=false
```

See [README_AIRFLOW_SPARK.md](README_AIRFLOW_SPARK.md#automated-data-lineage-with-neo4j) for detailed lineage documentation.

## Getting Help

- **Airflow UI**: http://localhost:8082 - View DAG runs and logs
- **Spark UI**: http://localhost:8080 - Monitor Spark jobs
- **MinIO Console**: http://localhost:9001 - Browse data files
- **Neo4j Browser**: http://localhost:7474 - View data lineage (if Neo4j is running)
- **Logs**: `docker-compose logs -f <service>`

## Ready to Go!

You're all set! You now have:
- A running data lakehouse
- Bronze layer with ingested data
- Empty silver and gold layers ready for your transformations
- Airflow orchestrating workflows
- Jupyter for development
- Automatic data lineage tracking to Neo4j

Start building your silver and gold transformations and create a complete ETL pipeline!
