# Airflow DAG Knowledge Graph

This module extracts DAG metadata from Apache Airflow and loads it into Memgraph to create a knowledge graph of your data pipeline architecture.

## Overview

The `parse-airflow.py` script connects to your Airflow instance via REST API, parses DAG Python files to extract tasks and dependencies, and loads the data directly into Memgraph for visualization and analysis.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│              Knowledge Graph Creation Flow              │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  Airflow REST API                                        │
│       ↓                                                  │
│  Fetch DAG Metadata & Source Code                        │
│       ↓                                                  │
│  Parse DAG Python Files (AST)                            │
│       ↓                                                  │
│  Extract Tasks, Dependencies & Spark Configurations      │
│       ↓                                                  │
│  Memgraph (Graph Database)                               │
│    • DAG nodes                                           │
│    • Task nodes                                          │
│    • SparkJob nodes (with resource & dependency info)    │
│    • CONTAINS, EXECUTES, DEPENDS_ON relationships        │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

## Features

- **REST API Integration**: Fetches DAG information from Airflow API
- **AST Parsing**: Analyzes Python DAG files to extract task definitions and dependencies
- **Graph Database Storage**: Loads data directly into Memgraph (no intermediate JSON files)
- **Relationship Tracking**: Captures task dependencies, DAG triggers, and Spark job mappings
- **Spark Job Detection**: Identifies SparkSubmitOperator tasks and extracts comprehensive job configuration
- **Dependency Analysis**: Tracks Spark job dependencies (packages, jars, py_files) for environment analysis
- **Resource Tracking**: Captures executor memory, cores, and other resource configurations for optimization

## Prerequisites

### 1. Custom Airflow Plugin (Required)

The project includes a custom Airflow plugin (`plugins/dag_code.py`) that extends the REST API to expose DAG source code. This plugin is **automatically loaded** when Airflow starts with the plugin in the `plugins/` directory.

**What it does:**
- Adds `/api/v1/dag_code/extract` endpoint to fetch all DAG source code
- Allows the parser to retrieve DAG Python files without file system access
- Uses Airflow's standard authentication (basic auth)

**Verify plugin is loaded:**
```bash
# Check plugin health
curl http://localhost:8082/api/v1/dag_code/health

# Should return: {"status": "healthy", "plugin": "dag_code_extractor", "version": "1.0.0"}
```

**Endpoints provided:**
- `GET /api/v1/dag_code/extract` - Get all DAG source codes
- `GET /api/v1/dag_code/extract/<fileloc>` - Get specific DAG by file location
- `GET /api/v1/dag_code/health` - Health check (no auth required)

### 2. Install Neo4j Driver

The script uses the Neo4j driver to connect to Memgraph:

```bash
# Using pip
pip install neo4j

# Using poetry (if in project)
poetry add neo4j
```

### 3. Start Memgraph

You can run Memgraph using Docker:

```bash
docker run -d \
  --name memgraph \
  -p 7687:7687 \
  -p 3000:3000 \
  memgraph/memgraph-platform
```

**Ports:**
- `7687`: Bolt protocol (for database connections)
- `3000`: Memgraph Lab UI (for visualization)

### 4. Verify Airflow is Running

Ensure your Airflow instance is accessible:

```bash
# Check Airflow webserver
curl http://localhost:8082/health

# Verify API access
curl -u admin:admin http://localhost:8082/api/v1/dags

# Verify custom plugin is loaded
curl http://localhost:8082/api/v1/dag_code/health
```

## Configuration

Edit the configuration section in `parse-airflow.py`:

```python
# Airflow configuration
AIRFLOW_API = "http://localhost:8082/api/v1"
USERNAME = "admin"
PASSWORD = "admin"

# Memgraph configuration
MEMGRAPH_URI = "bolt://localhost:7687"
MEMGRAPH_AUTH = ("", "")  # Empty for no authentication

# Spark operator detection
SPARK_OPERATOR_NAMES = {
    "SparkSubmitOperator",
    "DataprocSubmitJobOperator",
    "EmrAddStepsOperator",
}

# Spark job parameters to extract (focused on analysis)
SPARK_JOB_PARAMS = {
    # Core identification
    "application", "name", "conn_id",
    # Resource configuration
    "executor_cores", "executor_memory", "driver_memory", "num_executors",
    # Dependencies & libraries
    "packages", "py_files", "jars", "files",
    # Configuration & behavior
    "application_args", "env_vars", "conf",
}
```

## Usage

### Basic Usage

Run the script to extract DAGs and load into Memgraph:

```bash
python create-kb/parse-airflow.py
```

### Expected Output

```
🚀 Extracting Airflow DAG tasks and dependencies...

🔍 Found 3 DAGs in Airflow.
📄 Parsing DAG: ingest_bronze_data
   ✅ 3 total tasks (1 Spark jobs)
📄 Parsing DAG: bronze_to_silver_pipeline
   ✅ 8 total tasks (6 Spark jobs)
📄 Parsing DAG: silver_to_gold_pipeline
   ✅ 11 total tasks (9 Spark jobs)

📦 Total tasks found: 22 (16 Spark jobs)

📊 Spark Jobs Summary:
  • ingest_bronze_data.ingest_bronze_data
    - Application: /opt/spark-apps/ingest_bronze_data.py
    - Downstream: ['verify_ingestion']

============================================================
📊 MEMGRAPH INTEGRATION
============================================================
✅ Connected to Memgraph at bolt://localhost:7687
🗑️  Cleared existing data from Memgraph

📊 Loading 22 tasks from 3 DAGs into Memgraph...
   ✅ Created 3 DAG nodes
   ✅ Created 22 Task nodes
   ✅ Created 16 SparkJob nodes
   ✅ Created 19 dependency relationships
   ✅ Created 1 TRIGGERS relationships
   ✅ Created indexes

🎉 Successfully loaded data into Memgraph!
   📍 Access Memgraph Lab at: http://localhost:3000
   💡 Example queries:
      - View DAGs and Tasks: MATCH (d:DAG)-[:CONTAINS]->(t:Task) RETURN d, t LIMIT 25
      - View Spark Jobs: MATCH (t:Task)-[:EXECUTES]->(sj:SparkJob) RETURN t, sj LIMIT 25
      - Full pipeline: MATCH (d:DAG)-[:CONTAINS]->(t:Task)-[:EXECUTES]->(sj:SparkJob) RETURN d, t, sj LIMIT 25
```

## Graph Schema

### Nodes

**DAG Node**
```cypher
(:DAG {
  dag_id: string,           // Unique DAG identifier
  total_tasks: int,         // Total number of tasks
  spark_tasks: int,         // Number of Spark tasks
  updated_at: timestamp     // Last update time
})
```

**Task Node**
```cypher
(:Task {
  task_id: string,          // Task identifier
  dag_id: string,           // Parent DAG
  operator_type: string,    // Airflow operator class name
  is_spark_task: boolean,   // Whether it's a Spark job
  spark_job: string,        // Path to Spark application (if applicable)
  params: json,             // Task parameters
  updated_at: timestamp     // Last update time
})
```

**SparkJob Node** (New!)
```cypher
(:SparkJob {
  job_id: string,           // Unique job identifier
  // Core identification
  application: string,      // Path to Spark application (e.g., /opt/spark-apps/my_job.py)
  name: string,            // Job name
  conn_id: string,         // Spark connection ID
  // Resource configuration
  executor_cores: int,     // Number of cores per executor
  executor_memory: string, // Memory per executor (e.g., "2g")
  driver_memory: string,   // Driver memory (e.g., "1g")
  num_executors: int,      // Number of executors
  // Dependencies & Libraries
  packages: string,        // Maven packages (e.g., org.apache.spark:spark-sql-kafka)
  py_files: string,        // Python dependencies (.zip, .egg, .py files)
  jars: string,           // JAR file dependencies
  files: string,          // Additional files (configs, lookup tables)
  // Configuration & Behavior
  application_args: json,  // Arguments passed to the Spark application
  env_vars: json,         // Environment variables
  conf: json,             // Spark configuration dictionary
  updated_at: timestamp   // Last update time
})
```

### Relationships

**CONTAINS**: Links DAG to its tasks
```cypher
(DAG)-[:CONTAINS]->(Task)
```

**EXECUTES**: Links Task to SparkJob configuration
```cypher
(Task)-[:EXECUTES]->(SparkJob)
```

**DEPENDS_ON**: Task dependency relationship
```cypher
(Task)-[:DEPENDS_ON]->(Task)
```

**TRIGGERS**: DAG triggering relationship (from TriggerDagRunOperator)
```cypher
(Task)-[:TRIGGERS]->(DAG)
```

### Why SparkJob Nodes Matter

The SparkJob nodes provide deep insights into your Spark applications:

**Resource Management:**
- Identify over/under-provisioned jobs by analyzing executor memory and core allocations
- Compare resource usage across similar jobs to standardize configurations
- Track total compute resources used across all pipelines

**Dependency Management:**
- Know which jobs use external packages (Kafka connectors, Delta Lake, etc.)
- Track custom Python modules and JAR dependencies
- Identify jobs that need specific configuration files

**Environment Analysis:**
- See which jobs use environment variables for configuration
- Understand Spark configuration patterns across your organization
- Identify jobs with special requirements (specific Spark versions, connectors)

**Optimization Opportunities:**
- Find duplicate or similar job configurations
- Identify jobs that could share dependencies or resources
- Track configuration drift between development and production

## Querying the Knowledge Graph

### Access Memgraph Lab

Open your browser and navigate to:
```
http://localhost:3000
```

### Example Queries

**1. View all DAGs and their tasks:**
```cypher
MATCH (d:DAG)-[:CONTAINS]->(t:Task)
RETURN d, t
LIMIT 50;
```

**2. Find all Spark jobs with their configurations:**
```cypher
MATCH (t:Task)-[:EXECUTES]->(sj:SparkJob)
RETURN t.dag_id, t.task_id, sj.application, sj.executor_memory, sj.num_executors;
```

**3. Find Spark jobs using specific packages (e.g., Kafka):**
```cypher
MATCH (sj:SparkJob)
WHERE sj.packages CONTAINS 'kafka'
RETURN sj.application, sj.packages;
```

**4. Find resource-intensive Spark jobs:**
```cypher
MATCH (sj:SparkJob)
WHERE sj.num_executors > 5 OR sj.executor_memory >= '4g'
RETURN sj.application, sj.num_executors, sj.executor_memory, sj.driver_memory
ORDER BY sj.num_executors DESC;
```

**5. Analyze job dependencies and libraries:**
```cypher
MATCH (sj:SparkJob)
WHERE sj.packages IS NOT NULL OR sj.jars IS NOT NULL OR sj.py_files IS NOT NULL
RETURN sj.application, sj.packages, sj.py_files, sj.jars;
```

**6. Find task dependencies for a specific DAG:**
```cypher
MATCH (d:DAG {dag_id: 'bronze_to_silver_pipeline'})-[:CONTAINS]->(t:Task)
OPTIONAL MATCH (t)-[:DEPENDS_ON]->(upstream:Task)
RETURN t.task_id, collect(upstream.task_id) as upstream_tasks;
```

**7. Trace complete pipeline with Spark jobs:**
```cypher
MATCH (d:DAG)-[:CONTAINS]->(t:Task)-[:EXECUTES]->(sj:SparkJob)
OPTIONAL MATCH (t)-[:DEPENDS_ON]->(upstream:Task)
RETURN d.dag_id, t.task_id, sj.application, upstream.task_id as upstream_task;
```

**8. Find all tasks that trigger other DAGs:**
```cypher
MATCH (t:Task)-[:TRIGGERS]->(d:DAG)
RETURN t.dag_id, t.task_id, d.dag_id as triggered_dag;
```

**9. Find the complete dependency chain for a task:**
```cypher
MATCH path = (t:Task {task_id: 'ingest_bronze_data'})-[:DEPENDS_ON*]->(upstream:Task)
RETURN path;
```

**10. Count tasks by operator type:**
```cypher
MATCH (t:Task)
RETURN t.operator_type, count(*) as count
ORDER BY count DESC;
```

**11. Find DAGs with the most Spark jobs:**
```cypher
MATCH (d:DAG)
RETURN d.dag_id, d.spark_tasks
ORDER BY d.spark_tasks DESC;
```

**12. Visualize the entire pipeline with Spark jobs:**
```cypher
MATCH (d:DAG)-[:CONTAINS]->(t:Task)
OPTIONAL MATCH (t)-[:EXECUTES]->(sj:SparkJob)
OPTIONAL MATCH (t)-[r:DEPENDS_ON|TRIGGERS]->(related)
RETURN d, t, sj, r, related;
```

**13. Find Spark jobs with environment variables:**
```cypher
MATCH (sj:SparkJob)
WHERE sj.env_vars IS NOT NULL
RETURN sj.application, sj.env_vars;
```

**14. Compare resource allocation across jobs:**
```cypher
MATCH (sj:SparkJob)
WHERE sj.executor_memory IS NOT NULL
RETURN sj.application,
       sj.num_executors,
       sj.executor_memory,
       sj.driver_memory
ORDER BY sj.num_executors DESC;
```

## Code Structure

The refactored code follows a clean object-oriented design:

### Classes

**AirflowClient** - Manages Airflow REST API interactions
- `get()`: Make GET requests to Airflow API
- `get_dags()`: Fetch all DAG IDs and file locations
- `get_dag_code()`: Retrieve DAG source code

**DagASTParser** - Parses DAG Python files using AST
- `parse()`: Main parsing entry point
- `_extract_tasks()`: Extract task definitions from DAG code
- `_extract_dependencies()`: Extract task dependencies (>>, <<, methods)
- `_extract_bitshift_dependencies()`: Handle >> and << operators
- `_extract_method_dependencies()`: Handle set_upstream/set_downstream

**MemgraphClient** - Manages Memgraph database operations
- `connect()`: Establish connection to Memgraph
- `clear_all()`: Clear existing graph data
- `load_tasks()`: Load tasks into graph
- `_create_dag_nodes()`: Create DAG nodes
- `_create_task_nodes()`: Create task nodes
- `_create_spark_job_nodes()`: Create SparkJob nodes with full configuration
- `_create_dependencies()`: Create dependency relationships
- `_create_triggers()`: Create trigger relationships
- `_create_indexes()`: Create indexes for query performance

## Detected Task Patterns

The AST parser can detect:

1. **Operator instantiation**
   ```python
   task1 = SparkSubmitOperator(
       task_id='process_data',
       application='/opt/spark-apps/job.py',
       # ...
   )
   ```

2. **Bitshift dependencies**
   ```python
   task1 >> task2 >> task3
   task1 >> [task2, task3] >> task4
   ```

3. **Method-based dependencies**
   ```python
   task1.set_downstream(task2)
   task2.set_upstream(task1)
   ```

4. **DAG triggering**
   ```python
   trigger = TriggerDagRunOperator(
       task_id='trigger_next_dag',
       trigger_dag_id='downstream_dag',
   )
   ```

## Troubleshooting

### Neo4j Driver Not Found

```bash
# Install the neo4j driver
pip install neo4j
```

### Cannot Connect to Memgraph

```bash
# Check if Memgraph is running
docker ps | grep memgraph

# Check Memgraph logs
docker logs memgraph

# Verify port is accessible
nc -zv localhost 7687
```

### Plugin Not Loaded

If you get errors about the `/api/v1/dag_code/extract` endpoint not existing:

```bash
# Check if plugin is loaded
curl http://localhost:8082/api/v1/dag_code/health

# If not working, verify plugin file exists
ls -la plugins/dag_code.py

# Check Airflow logs for plugin loading errors
docker-compose logs airflow-webserver | grep -i plugin

# Restart Airflow to reload plugins
docker-compose restart airflow-webserver airflow-scheduler
```

### Airflow API Returns 401

- Verify credentials in configuration
- Check Airflow authentication settings
- Try accessing API manually:
  ```bash
  curl -u admin:admin http://localhost:8082/api/v1/dags
  ```

### No Tasks Found in DAG

- Verify DAG file has task definitions
- Check DAG is using supported operators
- Ensure tasks are properly assigned to variables
- Review parsing warnings in console output

### Connection Refused to Airflow

- Ensure Airflow webserver is running
- Verify correct URL and port
- Check docker-compose services:
  ```bash
  docker-compose ps airflow-webserver
  ```

## Use Cases

### 1. Pipeline Documentation
Generate automatic documentation of your data pipeline architecture without maintaining manual diagrams.

### 2. Impact Analysis
Understand which downstream tasks are affected when you modify a specific task or DAG.

### 3. Dependency Visualization
Visualize complex task dependencies across multiple DAGs using Memgraph Lab's graph visualization.

### 4. Spark Job Inventory
Maintain a comprehensive inventory of all Spark jobs across your pipeline with their configurations, dependencies, and resource requirements.

### 5. Resource Optimization
Analyze resource allocation across Spark jobs (executor memory, cores, number of executors) to identify over-provisioned or under-provisioned jobs.

### 6. Dependency Management
Track external dependencies (packages, jars, py_files) across all Spark jobs to understand library usage and identify version conflicts.

### 7. Environment Analysis
Identify Spark jobs that use environment variables or specific configurations, making it easier to replicate environments or debug configuration issues.

### 8. Pipeline Optimization
Identify bottlenecks and optimization opportunities by analyzing task dependencies and execution patterns.

### 9. Data Lineage
Track data flow through bronze → silver → gold layers by following task relationships.

### 10. Library Usage Analysis
Find all Spark jobs using specific libraries (e.g., Kafka, Delta Lake, Iceberg) to understand technology adoption across your pipeline.

## Integration with CI/CD

Add this to your CI/CD pipeline to automatically update the knowledge graph:

```yaml
# .github/workflows/update-knowledge-graph.yml
name: Update Knowledge Graph

on:
  push:
    paths:
      - 'dags/**'
      - 'spark-apps/**'

jobs:
  update-graph:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'

      - name: Install dependencies
        run: pip install neo4j requests

      - name: Update knowledge graph
        run: python create-kb/parse-airflow.py
        env:
          AIRFLOW_API: ${{ secrets.AIRFLOW_URL }}
          MEMGRAPH_URI: ${{ secrets.MEMGRAPH_URI }}
```

## Future Enhancements

- **Incremental Updates**: Only update changed DAGs instead of full refresh
- **Version History**: Track changes to DAGs and SparkJob configurations over time
- **Performance Metrics**: Add task duration, success rate, and resource utilization data from Spark history server
- **Data Lineage**: Link SparkJobs to data assets (tables, files, S3 buckets) they read/write
- **Custom Metadata**: Support for custom task metadata annotations
- **Export Functionality**: Export graph to other formats (GraphML, JSON, etc.)
- **Cost Analysis**: Integrate with cloud provider APIs to calculate job costs based on resource usage
- **Duplicate Detection**: Identify Spark jobs with similar configurations or dependencies
- **Optimization Recommendations**: Suggest resource optimizations based on historical patterns

## Technical Details: Custom Airflow Plugin

### How the Plugin Works

The `plugins/dag_code.py` file is a custom Airflow plugin that extends the REST API to expose DAG source code:

**Plugin Architecture:**
```python
# Creates a Flask Blueprint with custom endpoints
dag_code_bp = Blueprint("dag_code_extractor", ...)

# Registers with Airflow's plugin system
class DagCodeExtractorPlugin(AirflowPlugin):
    name = "dag_code_extractor"
    flask_blueprints = [dag_code_bp]
```

**Database Access:**
The plugin queries Airflow's internal `dag_code` table:
```python
@provide_session
def extract_dag_code(session: Session = None):
    dag_codes = session.query(DagCode).all()
    # Returns: fileloc, source_code, last_updated, etc.
```

**Authentication:**
Uses Airflow's standard basic authentication:
```python
@basic_auth
def extract_dag_code(...):
    # Requires valid Airflow credentials
```

**API Endpoints:**

1. **GET /api/v1/dag_code/extract**
   - Returns all DAG source codes
   - Supports pagination: `?limit=100&offset=0`
   - Supports filtering: `?fileloc=/path/to/dag.py`

2. **GET /api/v1/dag_code/extract/<fileloc>**
   - Returns specific DAG by file path
   - Example: `/api/v1/dag_code/extract/opt/airflow/dags/my_dag.py`

3. **GET /api/v1/dag_code/health**
   - Health check endpoint (no auth required)
   - Returns plugin status and version

### Why This Plugin is Needed

Airflow's standard REST API doesn't expose the DAG source code directly. The `dag_code` table exists in Airflow's database but has no REST API endpoint. This plugin:

1. **Enables Remote Parsing**: Allows `parse-airflow.py` to run from any machine without filesystem access to DAG files
2. **Uses Existing Data**: Leverages Airflow's internal DAG code cache instead of reading files directly
3. **Maintains Security**: Uses Airflow's authentication system
4. **Provides Consistency**: Gets the exact code Airflow is using (not potentially stale files)

### Plugin Installation

Airflow automatically discovers and loads plugins from the `plugins/` directory:

```bash
# Airflow looks for plugins in:
$AIRFLOW_HOME/plugins/

# In this project (via docker-compose):
./plugins/ → /opt/airflow/plugins/
```

**No installation steps needed** - just ensure the file exists in the plugins directory and restart Airflow.

### Customizing the Plugin

You can modify the plugin to add features:

**Add more metadata:**
```python
result = {
    'fileloc': dag_code.fileloc,
    'source_code': dag_code.source_code,
    'last_updated': dag_code.last_updated,
    'line_count': len(dag_code.source_code.split('\n')),  # Custom field
}
```

**Add caching:**
```python
from functools import lru_cache

@lru_cache(maxsize=100)
def get_dag_code_cached(fileloc):
    # Cache results for better performance
    pass
```

**Add filtering:**
```python
# Only return DAGs matching certain patterns
if not fileloc.startswith('/opt/airflow/dags/production/'):
    continue
```

## Related Documentation

- [Main Project README](../README.md) - Overall project documentation
- [Airflow Integration Guide](../README_AIRFLOW_SPARK.md) - Airflow + Spark setup
- [Memgraph Documentation](https://memgraph.com/docs) - Memgraph query language
- [Airflow REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html) - Airflow API reference
- [Airflow Plugins](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html) - How to write Airflow plugins
