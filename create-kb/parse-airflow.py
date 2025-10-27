"""
Airflow DAG Parser and Memgraph Knowledge Base Builder

This script extracts DAG metadata from Airflow's REST API and loads it into Memgraph
as a graph database for analysis.

Graph Structure:
    DAG --[CONTAINS]--> Task --[EXECUTES]--> SparkJob
    Task --[DEPENDS_ON]--> Task
    Task --[TRIGGERS]--> DAG

Node Types:
    - DAG: Airflow DAG with metadata (total_tasks, spark_tasks)
    - Task: Individual task with operator info and dependencies
    - SparkJob: Spark job configuration (application, resources, args)

SparkJob Properties (focused on analysis):
    - application: Path to the Spark application
    - name: Job identifier
    - conn_id: Spark connection
    - executor_cores, executor_memory, driver_memory, num_executors: Resource config
    - packages, py_files, jars, files: Dependencies and libraries
    - application_args: Runtime arguments
    - env_vars: Environment variables
    - conf: Spark configuration dictionary
"""

import json
from typing import Dict, List, Optional, Any

import requests

try:
    from neo4j import GraphDatabase

    NEO4J_AVAILABLE = True
except ImportError:
    print("⚠️ neo4j driver not installed. Install with: pip install neo4j")
    GraphDatabase = None
    NEO4J_AVAILABLE = False

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
AIRFLOW_API = "http://localhost:8082/api/v1"
USERNAME = "admin"
PASSWORD = "admin"

MEMGRAPH_URI = "bolt://localhost:7687"
MEMGRAPH_AUTH = ("", "")

SPARK_OPERATOR_NAMES = {
    "SparkSubmitOperator",
    "DataprocSubmitJobOperator",
    "EmrAddStepsOperator",
}

SPARK_JOB_PARAMS = {
    # Core job identification
    "application",  # Path to the Spark application (e.g., /opt/spark-apps/my_job.py)
    "name",  # Job name for identification
    "conn_id",  # Spark connection ID
    # Resource configuration (for analysis)
    "executor_cores",  # Number of cores per executor
    "executor_memory",  # Memory per executor (e.g., "2g")
    "driver_memory",  # Driver memory (e.g., "1g")
    "num_executors",  # Number of executors
    # Dependencies & Libraries (important for understanding job requirements)
    "packages",  # Maven packages (e.g., spark-sql-kafka, delta-core)
    "py_files",  # Python dependencies (.zip, .egg, .py files)
    "jars",  # JAR file dependencies
    "files",  # Additional files (configs, lookup tables)
    # Job behavior & configuration
    "application_args",  # Arguments passed to the Spark application
    "env_vars",  # Environment variables
    "conf",  # Spark configuration dictionary
}


# --------------------------------------------------
# AIRFLOW API CLIENT
# --------------------------------------------------
class AirflowClient:
    """Client for interacting with Airflow REST API."""

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.auth = (username, password)

    def get(self, endpoint: str) -> dict:
        """Make a GET request to the Airflow API."""
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, auth=self.auth)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {url} → {response.status_code}")
            return {}
        return response.json()

    def get_dags(self) -> Dict[str, str]:
        """Fetch all DAG IDs and their file locations."""
        dags = self.get("/dags").get("dags", [])
        return {d["dag_id"]: d["fileloc"] for d in dags}

    def get_dag_code(self, fileloc: str) -> Optional[str]:
        """Fetch source code for a DAG by file location."""
        codes = self.get("/dag_code/extract").get("dag_codes", [])
        matching_codes = [code["source_code"] for code in codes if code["fileloc"] == fileloc]
        return matching_codes[0] if matching_codes else None


# --------------------------------------------------
# AST PARSER FOR DAG FILES
# --------------------------------------------------
class DagASTParser:
    """Parses Airflow DAG Python files using AST to extract tasks and dependencies."""

    def __init__(self, dag_id: str, dag_code: str):
        self.dag_id = dag_id
        self.dag_code = dag_code
        self.tasks: Dict[str, Dict] = {}
        self.task_var_to_id: Dict[str, str] = {}
        self.dependencies: List[tuple] = []

    def parse(self) -> Dict[str, Any]:
        """Parse the DAG code and return tasks with dependencies."""
        import ast

        try:
            tree = ast.parse(self.dag_code)
        except Exception as e:
            print(f"⚠️ Failed to parse DAG {self.dag_id}: {e}")
            return {"tasks": [], "dependencies": []}

        self._extract_tasks(tree)
        self._extract_dependencies(tree)
        return self._build_result()

    def _extract_tasks(self, tree):
        """First pass: Extract all task definitions."""
        import ast

        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                operator_name = self._get_operator_name(node.value)
                if operator_name and "Operator" in operator_name:
                    task_info = self._extract_task_info(node.value, operator_name)
                    if task_info:
                        task_id = task_info["task_id"]
                        self.tasks[task_id] = task_info

                        # Map variable name to task_id
                        if node.targets:
                            var_name = self._get_var_name(node.targets[0])
                            if var_name:
                                self.task_var_to_id[var_name] = task_id

    def _extract_dependencies(self, tree):
        """Second pass: Extract task dependencies from bitshift operators and methods."""

        for stmt in tree.body:
            self._process_statement(stmt)

    def _process_statement(self, stmt):
        """Process a statement to extract dependencies."""
        import ast

        if isinstance(stmt, ast.Expr):
            if isinstance(stmt.value, ast.BinOp) and isinstance(stmt.value.op, (ast.RShift, ast.LShift)):
                deps = self._extract_bitshift_dependencies(stmt.value)
                self.dependencies.extend(deps)
            elif isinstance(stmt.value, ast.Call) and isinstance(stmt.value.func, ast.Attribute):
                method_name = stmt.value.func.attr
                if method_name in ["set_downstream", "set_upstream"]:
                    deps = self._extract_method_dependencies(stmt.value)
                    self.dependencies.extend(deps)

    def _build_result(self) -> Dict[str, Any]:
        """Build the final result with tasks and their relationships."""
        # Initialize upstream/downstream lists
        for task_id in self.tasks:
            self.tasks[task_id]["upstream"] = []
            self.tasks[task_id]["downstream"] = []

        # Populate relationships
        for upstream_id, downstream_id in self.dependencies:
            if upstream_id in self.tasks and downstream_id in self.tasks:
                self.tasks[upstream_id]["downstream"].append(downstream_id)
                self.tasks[downstream_id]["upstream"].append(upstream_id)

        # Build task records
        all_tasks = []
        for task_id, info in self.tasks.items():
            is_spark_task = info["operator_type"] in SPARK_OPERATOR_NAMES
            task_record = {
                "dag_id": self.dag_id,
                "task_id": task_id,
                "operator_type": info["operator_type"],
                "is_spark_task": is_spark_task,
                "spark_job": info.get("application") if is_spark_task else None,
                "spark_params": info.get("spark_params", {}) if is_spark_task else {},
                "trigger_dag_id": info.get("trigger_dag_id"),
                "params": info.get("params", {}),
                "upstream": info["upstream"],
                "downstream": info["downstream"],
            }
            all_tasks.append(task_record)

        return {"tasks": all_tasks, "dependencies": self.dependencies}

    # Value extraction methods
    @staticmethod
    def _get_operator_name(call_node):
        """Extract operator name from call node."""
        import ast
        if isinstance(call_node.func, ast.Name):
            return call_node.func.id
        elif isinstance(call_node.func, ast.Attribute):
            return call_node.func.attr
        return None

    @staticmethod
    def _get_var_name(node):
        """Extract variable name from assignment target."""
        import ast
        if isinstance(node, ast.Name):
            return node.id
        return None

    @staticmethod
    def _extract_value(node):
        """Extract value from AST node."""
        import ast
        if isinstance(node, ast.Constant):
            return node.value
        elif isinstance(node, ast.Str):
            return node.s
        elif isinstance(node, ast.Num):
            return node.n
        elif isinstance(node, ast.List):
            return [DagASTParser._extract_value(item) for item in node.elts]
        elif isinstance(node, ast.Dict):
            return {
                DagASTParser._extract_value(k): DagASTParser._extract_value(v)
                for k, v in zip(node.keys, node.values)
                if k is not None
            }
        elif isinstance(node, ast.Name):
            return f"<variable:{node.id}>"
        return str(type(node).__name__)

    def _extract_task_info(self, call_node, operator_name: str) -> Optional[Dict]:
        """Extract task_id, operator type, and parameters from operator call."""
        task_info = {
            "operator_type": operator_name,
            "task_id": None,
            "application": None,
            "trigger_dag_id": None,
            "params": {},
            "spark_params": {},
        }

        is_spark_operator = operator_name in SPARK_OPERATOR_NAMES

        for keyword in call_node.keywords:
            key = keyword.arg
            value = self._extract_value(keyword.value)

            if key == "task_id":
                task_info["task_id"] = value
            elif key == "trigger_dag_id":
                task_info["trigger_dag_id"] = value
            elif is_spark_operator and key in SPARK_JOB_PARAMS:
                # Store Spark-specific parameters separately
                task_info["spark_params"][key] = value
                if key == "application":
                    task_info["application"] = value
            else:
                task_info["params"][key] = value

        return task_info if task_info["task_id"] else None

    # Dependency extraction methods
    @staticmethod
    def _get_task_names(node):
        """Extract task variable names from node (handles single tasks and lists)."""
        import ast
        if isinstance(node, ast.Name):
            return [node.id]
        elif isinstance(node, ast.List):
            return [item.id for item in node.elts if isinstance(item, ast.Name)]
        elif isinstance(node, ast.BinOp):
            left = DagASTParser._get_task_names(node.left)
            right = DagASTParser._get_task_names(node.right)
            return left + right
        return []

    @staticmethod
    def _get_rightmost_tasks(node):
        """Get the rightmost task(s) from a potentially chained expression."""
        import ast
        if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
            return DagASTParser._get_task_names(node.right)
        return DagASTParser._get_task_names(node)

    def _extract_bitshift_dependencies(self, node):
        """Extract dependencies from >> or << operators."""
        import ast
        dependencies = []

        # Handle chained operations recursively
        if isinstance(node.left, ast.BinOp) and isinstance(node.left.op, (ast.RShift, ast.LShift)):
            dependencies.extend(self._extract_bitshift_dependencies(node.left))

        # Extract direct dependency from this node
        left_tasks = self._get_rightmost_tasks(node.left)
        right_tasks = self._get_task_names(node.right)

        # >> means left -> right (left is upstream)
        if isinstance(node.op, ast.RShift):
            for left_var in left_tasks:
                for right_var in right_tasks:
                    left_id = self.task_var_to_id.get(left_var)
                    right_id = self.task_var_to_id.get(right_var)
                    if left_id and right_id:
                        dependencies.append((left_id, right_id))

        # << means right -> left (right is upstream)
        elif isinstance(node.op, ast.LShift):
            for right_var in right_tasks:
                for left_var in left_tasks:
                    right_id = self.task_var_to_id.get(right_var)
                    left_id = self.task_var_to_id.get(left_var)
                    if right_id and left_id:
                        dependencies.append((right_id, left_id))

        return dependencies

    def _extract_method_dependencies(self, call_node):
        """Extract dependencies from set_downstream() or set_upstream() calls."""
        import ast
        dependencies = []

        if not isinstance(call_node.func, ast.Attribute):
            return dependencies

        method_name = call_node.func.attr

        if isinstance(call_node.func.value, ast.Name):
            caller_var = call_node.func.value.id
            caller_id = self.task_var_to_id.get(caller_var)

            for arg in call_node.args:
                arg_vars = self._get_task_names(arg)
                for arg_var in arg_vars:
                    arg_id = self.task_var_to_id.get(arg_var)

                    if caller_id and arg_id:
                        if method_name == "set_downstream":
                            dependencies.append((caller_id, arg_id))
                        elif method_name == "set_upstream":
                            dependencies.append((arg_id, caller_id))

        return dependencies


# --------------------------------------------------
# DAG COLLECTION
# --------------------------------------------------
def collect_airflow_tasks(client: AirflowClient) -> List[Dict]:
    """Collect all tasks from all DAGs in Airflow."""
    all_tasks = []

    dag_files = client.get_dags()
    print(f"🔍 Found {len(dag_files)} DAGs in Airflow.")

    for dag_id, fileloc in dag_files.items():
        print(f"📄 Parsing DAG: {dag_id}")
        try:
            dag_code = client.get_dag_code(fileloc)
            if not dag_code:
                print(f"   ⚠️ Could not retrieve code for {dag_id}")
                continue

            parser = DagASTParser(dag_id, dag_code)
            parsed_dag = parser.parse()

            tasks = parsed_dag.get("tasks", [])
            spark_count = sum(1 for t in tasks if t.get("is_spark_task"))

            if tasks:
                print(f"   ✅ {len(tasks)} total tasks ({spark_count} Spark jobs)")
                all_tasks.extend(tasks)
            else:
                print(f"   No tasks found in {dag_id}")
        except Exception as e:
            print(f"   ⚠️ Error processing DAG {dag_id}: {e}")
            continue

    return all_tasks


# --------------------------------------------------
# MEMGRAPH CLIENT
# --------------------------------------------------
class MemgraphClient:
    """Client for interacting with Memgraph graph database."""

    def __init__(self, uri: str, auth: tuple[str, str]):
        self.uri = uri
        self.auth = auth
        self.driver = None

    def connect(self) -> bool:
        """Connect to Memgraph and verify connectivity."""
        if not NEO4J_AVAILABLE:
            print("⚠️ Neo4j driver not available. Skipping graph database integration.")
            return False

        try:
            self.driver = GraphDatabase.driver(uri=self.uri, auth=self.auth)
            self.driver.verify_connectivity()
            print(f"✅ Connected to Memgraph at {self.uri}")
            return True
        except Exception as e:
            print(f"⚠️ Failed to connect to Memgraph: {e}")
            return False

    def clear_all(self):
        """Clear all existing data from Memgraph."""
        if not self.driver:
            return

        try:
            self.driver.execute_query("MATCH (n) DETACH DELETE n;", database_="memgraph")
            print("🗑️  Cleared existing data from Memgraph")
        except Exception as e:
            print(f"⚠️ Failed to clear Memgraph: {e}")

    def load_tasks(self, tasks: List[Dict]):
        """Load DAG tasks and dependencies into Memgraph."""
        if not self.driver:
            return

        try:
            # Group tasks by DAG
            dags_map = {}
            for task in tasks:
                dag_id = task["dag_id"]
                if dag_id not in dags_map:
                    dags_map[dag_id] = []
                dags_map[dag_id].append(task)

            print(f"\n📊 Loading {len(tasks)} tasks from {len(dags_map)} DAGs into Memgraph...")

            # Create DAG nodes
            self._create_dag_nodes(dags_map)

            # Create Task nodes
            self._create_task_nodes(tasks)

            # Create SparkJob nodes
            self._create_spark_job_nodes(tasks)

            # Create dependencies
            self._create_dependencies(tasks)

            # Create trigger relationships
            self._create_triggers(tasks)

            # Create indexes
            self._create_indexes()

            print(f"\n🎉 Successfully loaded data into Memgraph!")
            print(f"   📍 Access Memgraph Lab at: http://localhost:3000")
            print(f"\n   💡 Example Analysis Queries:")
            print(f"      - View all pipelines with resources:")
            print(f"        MATCH (d:DAG)-[:CONTAINS]->(t:Task)-[:EXECUTES]->(sj:SparkJob)")
            print(f"        RETURN d.dag_id, t.task_id, sj.application, sj.executor_memory, sj.num_executors")
            print(f"")
            print(f"      - Find jobs using specific packages (e.g., Kafka):")
            print(f"        MATCH (sj:SparkJob)")
            print(f"        WHERE sj.packages CONTAINS 'kafka'")
            print(f"        RETURN sj.application, sj.packages")
            print(f"")
            print(f"      - Find resource-intensive jobs:")
            print(f"        MATCH (sj:SparkJob)")
            print(f"        WHERE sj.num_executors > 5 OR sj.executor_memory >= '4g'")
            print(f"        RETURN sj.application, sj.num_executors, sj.executor_memory")
            print(f"")
            print(f"      - Analyze job dependencies:")
            print(f"        MATCH (sj:SparkJob)")
            print(f"        WHERE sj.packages IS NOT NULL OR sj.jars IS NOT NULL")
            print(f"        RETURN sj.application, sj.packages, sj.py_files, sj.jars")

        except Exception as e:
            print(f"⚠️ Error loading data to Memgraph: {e}")
            import traceback
            traceback.print_exc()

    def _create_dag_nodes(self, dags_map: Dict[str, List[Dict]]):
        """Create DAG nodes in Memgraph."""
        for dag_id, tasks in dags_map.items():
            spark_count = sum(1 for t in tasks if t.get("is_spark_task"))

            query = """
            MERGE (d:DAG {dag_id: $dag_id})
            SET d.name = $dag_id,
                d.total_tasks = $total_tasks,
                d.spark_tasks = $spark_tasks,
                d.updated_at = timestamp()
            """
            self.driver.execute_query(
                query,
                dag_id=dag_id,
                total_tasks=len(tasks),
                spark_tasks=spark_count,
                database_="memgraph"
            )

        print(f"   ✅ Created {len(dags_map)} DAG nodes")

    def _create_task_nodes(self, tasks: List[Dict]):
        """Create Task nodes and link them to DAG nodes."""
        for task in tasks:
            params_json = json.dumps(task.get("params", {}))

            query = """
            MATCH (d:DAG {dag_id: $dag_id})
            MERGE (t:Task {task_id: $task_id, dag_id: $dag_id})
            SET t.name = $task_id,
                t.operator_type = $operator_type,
                t.is_spark_task = $is_spark_task,
                t.spark_job = $spark_job,
                t.params = $params,
                t.updated_at = timestamp()
            MERGE (d)-[:CONTAINS]->(t)
            """
            self.driver.execute_query(
                query,
                dag_id=task["dag_id"],
                task_id=task["task_id"],
                operator_type=task["operator_type"],
                is_spark_task=task["is_spark_task"],
                spark_job=task.get("spark_job"),
                params=params_json,
                database_="memgraph"
            )

        print(f"   ✅ Created {len(tasks)} Task nodes")

    def _create_spark_job_nodes(self, tasks: List[Dict]):
        """Create SparkJob nodes and link them to Task nodes."""
        spark_tasks = [t for t in tasks if t.get("is_spark_task") and t.get("spark_params")]

        for task in spark_tasks:
            spark_params = task.get("spark_params", {})

            # Create unique job identifier
            job_name = spark_params.get("name", "unnamed-spark-job")
            application = spark_params.get("application", "")

            # Create a unique job ID based on application path
            if application:
                # Extract just the filename from the application path
                job_id = application.split("/")[-1] if "/" in application else application
                job_id = job_id.replace(".py", "").replace(".jar", "")
            else:
                job_id = f"{task['dag_id']}_{task['task_id']}_spark_job"

            # Convert complex types to JSON strings for storage
            conf = json.dumps(spark_params.get("conf")) if spark_params.get("conf") else None
            application_args = json.dumps(spark_params.get("application_args")) if spark_params.get(
                "application_args") else None
            env_vars = json.dumps(spark_params.get("env_vars")) if spark_params.get("env_vars") else None

            query = """
            MERGE (sj:SparkJob {job_id: $job_id})
            SET sj.name = $name,
                sj.application = $application,
                sj.conn_id = $conn_id,
                sj.executor_cores = $executor_cores,
                sj.executor_memory = $executor_memory,
                sj.driver_memory = $driver_memory,
                sj.num_executors = $num_executors,
                sj.packages = $packages,
                sj.py_files = $py_files,
                sj.jars = $jars,
                sj.files = $files,
                sj.application_args = $application_args,
                sj.env_vars = $env_vars,
                sj.conf = $conf,
                sj.updated_at = timestamp()
            """

            self.driver.execute_query(
                query,
                job_id=job_id,
                name=spark_params.get("name"),
                application=spark_params.get("application"),
                conn_id=spark_params.get("conn_id"),
                executor_cores=spark_params.get("executor_cores"),
                executor_memory=spark_params.get("executor_memory"),
                driver_memory=spark_params.get("driver_memory"),
                num_executors=spark_params.get("num_executors"),
                packages=spark_params.get("packages"),
                py_files=spark_params.get("py_files"),
                jars=spark_params.get("jars"),
                files=spark_params.get("files"),
                application_args=application_args,
                env_vars=env_vars,
                conf=conf,
                database_="memgraph"
            )

            # Create relationship between Task and SparkJob
            link_query = """
            MATCH (t:Task {task_id: $task_id, dag_id: $dag_id})
            MATCH (sj:SparkJob {job_id: $job_id})
            MERGE (t)-[:EXECUTES]->(sj)
            """

            self.driver.execute_query(
                link_query,
                task_id=task["task_id"],
                dag_id=task["dag_id"],
                job_id=job_id,
                database_="memgraph"
            )

        if spark_tasks:
            print(f"   ✅ Created {len(spark_tasks)} SparkJob nodes")

    def _create_dependencies(self, tasks: List[Dict]):
        """Create DEPENDS_ON relationships between tasks."""
        dep_count = 0
        for task in tasks:
            dag_id = task["dag_id"]
            task_id = task["task_id"]

            for upstream_task_id in task.get("upstream", []):
                query = """
                MATCH (upstream:Task {task_id: $upstream_task_id, dag_id: $dag_id})
                MATCH (downstream:Task {task_id: $downstream_task_id, dag_id: $dag_id})
                MERGE (downstream)-[:DEPENDS_ON]->(upstream)
                """
                self.driver.execute_query(
                    query,
                    dag_id=dag_id,
                    upstream_task_id=upstream_task_id,
                    downstream_task_id=task_id,
                    database_="memgraph"
                )
                dep_count += 1

        print(f"   ✅ Created {dep_count} dependency relationships")

    def _create_triggers(self, tasks: List[Dict]):
        """Create TRIGGERS relationships for TriggerDagRunOperator tasks."""
        trigger_count = 0
        for task in tasks:
            if task["operator_type"] == "TriggerDagRunOperator" and task.get("trigger_dag_id"):
                source_dag_id = task["dag_id"]
                source_task_id = task["task_id"]
                target_dag_id = task["trigger_dag_id"]

                # Ensure target DAG node exists
                self.driver.execute_query(
                    "MERGE (d:DAG {dag_id: $target_dag_id})",
                    target_dag_id=target_dag_id,
                    database_="memgraph"
                )

                # Create TRIGGERS relationship
                query = """
                MATCH (task:Task {task_id: $source_task_id, dag_id: $source_dag_id})
                MATCH (target:DAG {dag_id: $target_dag_id})
                MERGE (task)-[:TRIGGERS]->(target)
                """
                self.driver.execute_query(
                    query,
                    source_dag_id=source_dag_id,
                    source_task_id=source_task_id,
                    target_dag_id=target_dag_id,
                    database_="memgraph"
                )
                trigger_count += 1

        if trigger_count > 0:
            print(f"   ✅ Created {trigger_count} TRIGGERS relationships")

    def _create_indexes(self):
        """Create indexes for better query performance."""
        try:
            self.driver.execute_query("CREATE INDEX ON :DAG(dag_id);", database_="memgraph")
            self.driver.execute_query("CREATE INDEX ON :Task(task_id);", database_="memgraph")
            self.driver.execute_query("CREATE INDEX ON :Task(dag_id);", database_="memgraph")
            self.driver.execute_query("CREATE INDEX ON :SparkJob(job_id);", database_="memgraph")
            print("   ✅ Created indexes")
        except Exception:
            # Indexes might already exist
            pass

    def close(self):
        """Close the connection to Memgraph."""
        if self.driver:
            self.driver.close()


# --------------------------------------------------
# MAIN ENTRYPOINT
# --------------------------------------------------
def main():
    """Main workflow: Extract DAG data from Airflow and load into Memgraph."""

    # 1. Connect to Airflow and collect tasks
    print("🚀 Extracting Airflow DAG tasks and dependencies...\n")
    airflow = AirflowClient(AIRFLOW_API, USERNAME, PASSWORD)
    tasks = collect_airflow_tasks(airflow)

    # 2. Print summary
    spark_count = sum(1 for t in tasks if t.get("is_spark_task"))
    print(f"\n📦 Total tasks found: {len(tasks)} ({spark_count} Spark jobs)")

    spark_tasks = [t for t in tasks if t.get("is_spark_task")]
    if spark_tasks:
        print("\n📊 Spark Jobs Summary:")
        for task in spark_tasks:
            print(f"  • {task['dag_id']}.{task['task_id']}")
            print(f"    - Application: {task['spark_job']}")

            spark_params = task.get("spark_params", {})
            if spark_params:
                print(f"    - Spark Configuration:")
                if spark_params.get("name"):
                    print(f"      • Name: {spark_params['name']}")
                if spark_params.get("conn_id"):
                    print(f"      • Connection: {spark_params['conn_id']}")

                # Resource configuration
                resources = []
                if spark_params.get("num_executors"):
                    resources.append(f"{spark_params['num_executors']} executors")
                if spark_params.get("executor_cores"):
                    resources.append(f"{spark_params['executor_cores']} cores/executor")
                if spark_params.get("executor_memory"):
                    resources.append(f"{spark_params['executor_memory']} memory/executor")
                if spark_params.get("driver_memory"):
                    resources.append(f"{spark_params['driver_memory']} driver memory")
                if resources:
                    print(f"      • Resources: {', '.join(resources)}")

                # Dependencies & Libraries
                dependencies = []
                if spark_params.get("packages"):
                    dependencies.append(f"packages: {spark_params['packages']}")
                if spark_params.get("py_files"):
                    dependencies.append(f"py_files: {spark_params['py_files']}")
                if spark_params.get("jars"):
                    dependencies.append(f"jars: {spark_params['jars']}")
                if spark_params.get("files"):
                    dependencies.append(f"files: {spark_params['files']}")
                if dependencies:
                    print(f"      • Dependencies: {', '.join(dependencies)}")

                # Arguments & Configuration
                if spark_params.get("application_args"):
                    print(f"      • Args: {spark_params['application_args']}")
                if spark_params.get("env_vars"):
                    print(f"      • Env Vars: {len(spark_params['env_vars'])} variables")
                if spark_params.get("conf"):
                    print(f"      • Spark Config: {len(spark_params['conf'])} parameters")

            if task.get("upstream"):
                print(f"    - Upstream: {task['upstream']}")
            if task.get("downstream"):
                print(f"    - Downstream: {task['downstream']}")

    # 3. Load into Memgraph
    print("\n" + "=" * 60)
    print("📊 MEMGRAPH INTEGRATION")
    print("=" * 60)

    memgraph = MemgraphClient(MEMGRAPH_URI, MEMGRAPH_AUTH)
    if memgraph.connect():
        memgraph.clear_all()
        memgraph.load_tasks(tasks)
        memgraph.close()


if __name__ == "__main__":
    main()
