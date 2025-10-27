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
│  Fetch DAG Metadata                                      │
│       ↓                                                  │
│  Parse DAG Python Files (AST)                            │
│       ↓                                                  │
│  Extract Tasks & Dependencies                            │
│       ↓                                                  │
│  Memgraph (Graph Database)                               │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

## Features

- **REST API Integration**: Fetches DAG information from Airflow API
- **AST Parsing**: Analyzes Python DAG files to extract task definitions and dependencies
- **Graph Database Storage**: Loads data directly into Memgraph (no intermediate JSON files)
- **Relationship Tracking**: Captures task dependencies, DAG triggers, and Spark job mappings
- **Spark Job Detection**: Identifies SparkSubmitOperator tasks and their applications

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
   ✅ Created 19 dependency relationships
   ✅ Created 1 TRIGGERS relationships
   ✅ Created indexes

🎉 Successfully loaded data into Memgraph!
   📍 Access Memgraph Lab at: http://localhost:3000
   💡 Example query: MATCH (d:DAG)-[:CONTAINS]->(t:Task) RETURN d, t LIMIT 25
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

### Relationships

**CONTAINS**: Links DAG to its tasks
```cypher
(DAG)-[:CONTAINS]->(Task)
```

**DEPENDS_ON**: Task dependency relationship
```cypher
(Task)-[:DEPENDS_ON]->(Task)
```

**TRIGGERS**: DAG triggering relationship (from TriggerDagRunOperator)
```cypher
(Task)-[:TRIGGERS]->(DAG)
```

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

**2. Find all Spark jobs:**
```cypher
MATCH (t:Task)
WHERE t.is_spark_task = true
RETURN t.dag_id, t.task_id, t.spark_job;
```

**3. Find task dependencies for a specific DAG:**
```cypher
MATCH (d:DAG {dag_id: 'bronze_to_silver_pipeline'})-[:CONTAINS]->(t:Task)
OPTIONAL MATCH (t)-[:DEPENDS_ON]->(upstream:Task)
RETURN t.task_id, collect(upstream.task_id) as upstream_tasks;
```

**4. Find all tasks that trigger other DAGs:**
```cypher
MATCH (t:Task)-[:TRIGGERS]->(d:DAG)
RETURN t.dag_id, t.task_id, d.dag_id as triggered_dag;
```

**5. Find the complete dependency chain for a task:**
```cypher
MATCH path = (t:Task {task_id: 'ingest_bronze_data'})-[:DEPENDS_ON*]->(upstream:Task)
RETURN path;
```

**6. Count tasks by operator type:**
```cypher
MATCH (t:Task)
RETURN t.operator_type, count(*) as count
ORDER BY count DESC;
```

**7. Find DAGs with the most Spark jobs:**
```cypher
MATCH (d:DAG)
RETURN d.dag_id, d.spark_tasks
ORDER BY d.spark_tasks DESC;
```

**8. Visualize the entire pipeline:**
```cypher
MATCH (d:DAG)-[:CONTAINS]->(t:Task)
OPTIONAL MATCH (t)-[r:DEPENDS_ON|TRIGGERS]->(related)
RETURN d, t, r, related;
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
- `_create_dependencies()`: Create dependency relationships
- `_create_triggers()`: Create trigger relationships

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
Maintain an inventory of all Spark jobs across your pipeline with their file paths and dependencies.

### 5. Pipeline Optimization
Identify bottlenecks and optimization opportunities by analyzing task dependencies and execution patterns.

### 6. Data Lineage
Track data flow through bronze → silver → gold layers by following task relationships.

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
- **Version History**: Track changes to DAGs over time
- **Performance Metrics**: Add task duration and success rate data
- **Data Lineage**: Link to data assets (tables, files, etc.)
- **Custom Metadata**: Support for custom task metadata annotations
- **Export Functionality**: Export graph to other formats (GraphML, JSON, etc.)

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
