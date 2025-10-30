from __future__ import annotations

import os
import re
from typing import Dict, List, Optional, Set, Tuple


try:
    from neo4j import GraphDatabase as _Neo4jDriver
except Exception:
    _Neo4jDriver = None


def _dbg(msg: str) -> None:
    try:
        if os.getenv("LINEAGE_DEBUG", "true").lower() in ("1", "true", "yes"):
            print(f"[LINEAGE] {msg}")
    except Exception:
        pass


class Neo4jClient:
    def __init__(self, uri: str, user: Optional[str], password: Optional[str]) -> None:
        if _Neo4jDriver is None:
            raise ImportError("neo4j driver not installed; set LINEAGE_TO_NEO4J=false or install neo4j package")
        auth = (user, password) if user is not None and password is not None else None
        self._driver = _Neo4jDriver.driver(uri, auth=auth)
        _dbg(f"Neo4jClient initialized. URI={uri}, auth={'basic' if auth else 'none'}")
        try:
            with self._driver.session() as s:
                s.run("RETURN 1").consume()
            _dbg("Neo4j bolt connection OK")
        except Exception as e:
            _dbg(f"Neo4j connectivity test failed: {e}")

    def close(self) -> None:
        self._driver.close()

    def upsert_datasets(self, names: List[str]) -> None:
        if not names:
            return
        cypher = "UNWIND $names AS n MERGE (:Dataset {name: n})"
        with self._driver.session() as session:
            _dbg(f"Merging {len(names)} Dataset nodes")
            session.run(cypher, names=list(sorted(set(names))))

    def upsert_ctes(self, job: str, ctes: List[Dict]) -> None:
        if not ctes:
            return
        cypher_cte_nodes = "UNWIND $ctes AS c MERGE (:CTE {name: c.name, job: $job})"
        cypher_cte_edges = (
            "UNWIND $ctes AS c "
            "MATCH (cte:CTE {name: c.name, job: $job}) "
            "WITH cte, c.sources AS src_names "
            "UNWIND src_names AS src "
            "MATCH (s:Dataset {name: src}) "
            "MERGE (s)-[:FLOWS_TO]->(cte)"
        )
        cypher_cte_to_cte = (
            "UNWIND $ctes AS c "
            "MATCH (target_cte:CTE {name: c.name, job: $job}) "
            "WITH target_cte, c.cte_deps AS dep_names "
            "UNWIND dep_names AS dep "
            "MATCH (source_cte:CTE {name: dep, job: $job}) "
            "MERGE (source_cte)-[:FLOWS_TO]->(target_cte)"
        )
        with self._driver.session() as session:
            _dbg(f"Upserting {len(ctes)} CTE nodes")
            session.run(cypher_cte_nodes, ctes=ctes, job=job)
            _dbg("Creating CTE source edges")
            session.run(cypher_cte_edges, ctes=ctes, job=job)
            cte_with_deps = [c for c in ctes if c.get("cte_deps")]
            if cte_with_deps:
                _dbg("Creating CTE-to-CTE edges")
                session.run(cypher_cte_to_cte, ctes=cte_with_deps, job=job)

    def link_ctes_to_job(self, job: str, cte_names: List[str]) -> None:
        if not cte_names:
            return
        cypher = (
            "MERGE (j:SparkJob {name: $job}) "
            "WITH j "
            "UNWIND $cte_names AS cname "
            "MATCH (cte:CTE {name: cname, job: $job}) "
            "MERGE (cte)-[:FLOWS_TO]->(j)"
        )
        with self._driver.session() as session:
            _dbg(f"Linking {len(cte_names)} CTEs to job {job}")
            session.run(cypher, cte_names=list(sorted(set(cte_names))), job=job)

    def link_sources_to_job(self, job: str, sources: List[str]) -> None:
        if not sources:
            return
        cypher = (
            "MERGE (j:SparkJob {name: $job}) "
            "WITH j "
            "UNWIND $sources AS sname "
            "MATCH (s:Dataset {name: sname}) "
            "MERGE (s)-[:FLOWS_TO]->(j)"
        )
        with self._driver.session() as session:
            _dbg(f"Creating read-only edges from {len(sources)} sources to job {job}")
            session.run(cypher, sources=list(sorted(set(sources))), job=job)

    def link_job_writes(self, job: str, sources: List[str], destinations: List[str],
                        op: Optional[str] = None, managed: Optional[bool] = None,
                        location: Optional[str] = None) -> None:
        if not destinations:
            return
        pairs = [{"source": s, "dest": d} for s in set(sources or []) for d in set(destinations or [])]
        cypher = (
            "MERGE (j:SparkJob {name: $job}) "
            "WITH j, $pairs AS ps "
            "UNWIND ps AS p "
            "MATCH (s:Dataset {name: p.source}), (d:Dataset {name: p.dest}) "
            "MERGE (s)-[:FLOWS_TO]->(j) "
            "MERGE (j)-[w:WRITES_TO]->(d) "
            "SET w.op = $op "
            "SET w.managed = $managed "
            "SET w.location = $location"
        )
        with self._driver.session() as session:
            _dbg(f"Creating write edges for {len(pairs)} pairs (op={op}, managed={managed})")
            session.run(cypher, job=job, pairs=pairs, op=op, managed=managed, location=location)


class LineageTracker:
    def __init__(self, job_name: str, client: Optional[Neo4jClient]) -> None:
        self.job_name = job_name
        self.client = client
        self.sources: Set[str] = set()
        self.cte_lineage: Dict[str, Set[str]] = {}

    def add_source(self, name: str) -> None:
        if not name:
            return
        self.sources.add(name.lower())
        _dbg(f"Source added: {name}")

    def add_cte(self, cte_name: str, sources: Set[str]) -> None:
        if not cte_name:
            return
        self.cte_lineage[cte_name.lower()] = set(s.lower() for s in sources)
        _dbg(f"CTE added: {cte_name} -> {sources}")

    def emit_read_only(self) -> None:
        if not self.client:
            _dbg("Skipping emit: no Neo4j client")
            return
        src_list = list(sorted(self.sources))
        self.client.upsert_datasets(src_list)
        self.client.link_sources_to_job(self.job_name, src_list)

    def emit_writes(self, destinations: List[str], op: Optional[str] = None,
                    managed: Optional[bool] = None, location: Optional[str] = None) -> None:
        if not self.client:
            _dbg("Skipping emit: no Neo4j client")
            return
        dst_list = list(sorted(set(d.lower() for d in destinations if d)))
        src_list = list(sorted(self.sources))

        # Upsert datasets and CTEs
        self.client.upsert_datasets(src_list + dst_list)

        # CTE nodes and edges
        if self.cte_lineage:
            all_cte_names = set(self.cte_lineage.keys())
            ctes_for_graph = []
            for name, srcs in self.cte_lineage.items():
                table_srcs = [s for s in srcs if s not in all_cte_names]
                cte_deps = [s for s in srcs if s in all_cte_names]
                ctes_for_graph.append({"name": name, "sources": table_srcs, "cte_deps": cte_deps})
            self.client.upsert_ctes(self.job_name, ctes_for_graph)
            self.client.link_ctes_to_job(self.job_name, list(self.cte_lineage.keys()))

        # Link writes
        self.client.link_job_writes(self.job_name, src_list, dst_list, op=op, managed=managed, location=location)

    def reset(self) -> None:
        self.sources.clear()
        self.cte_lineage.clear()


def _parse_ctes(sql: str) -> Dict[str, Set[str]]:
    cte_lineage: Dict[str, Set[str]] = {}
    # Remove comments
    sql = re.sub(r'--[^\n]*', '', sql)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    lower = sql.lower()
    cte_pattern = r'(?:\bwith\s+|,\s*)(\w+)\s+as\s*\('
    for match in re.finditer(cte_pattern, lower, re.IGNORECASE):
        cte_name = match.group(1)
        start = match.end() - 1
        depth = 1
        i = start + 1
        end = start
        while i < len(lower) and depth > 0:
            ch = lower[i]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    end = i
                    break
            i += 1
        if end > start:
            body = lower[start + 1:end]
            srcs = set(re.findall(r"\bfrom\s+([\w\.]+)", body, re.IGNORECASE))
            srcs.update(re.findall(r"\bjoin\s+([\w\.]+)", body, re.IGNORECASE))
            if srcs:
                cte_lineage[cte_name] = set(srcs)
                _dbg(f"CTE parsed: {cte_name} -> {srcs}")
    return cte_lineage


def _parse_create_table(sql: str) -> Tuple[Optional[str], Optional[str], Optional[bool], Optional[str]]:
    # Returns (op, table, managed, location)
    # op in {CREATE, REPLACE, CTAS}; managed True/False; location if external path
    s = re.sub(r'--[^\n]*', '', sql)
    s = re.sub(r'/\*.*?\*/', '', s, flags=re.DOTALL)
    low = s.lower()
    # CREATE [OR REPLACE] [EXTERNAL] TABLE db.tbl ... [USING ...] [LOCATION '...']
    m = re.search(r"create\s+(or\s+replace\s+)?(external\s+)?table\s+([\w\.]+)", low)
    if m:
        op = 'REPLACE' if m.group(1) else 'CREATE'
        external = True if m.group(2) else False
        table = m.group(3)
        loc_match = re.search(r"location\s+'([^']+)'", low)
        location = loc_match.group(1) if loc_match else None
        managed = None if location is None and not external else (not external and location is None)
        # CTAS detection
        if re.search(r"\)\s*as\s*select|\bas\s*select\b", low):
            op = 'CTAS'
        return op, table, managed, location
    return None, None, None, None


def enable(spark) -> None:
    enabled = os.getenv("LINEAGE_TO_NEO4J", "true").lower() in ("1", "true", "yes")
    if not enabled:
        _dbg("Lineage disabled via LINEAGE_TO_NEO4J env var")
        return

    uri = os.getenv("NEO4J_URI", os.getenv("MEMGRAPH_URI", "bolt://neo4j:7687"))
    user = os.getenv("NEO4J_USER", os.getenv("MEMGRAPH_USER", "neo4j"))
    password = os.getenv("NEO4J_PASSWORD", os.getenv("MEMGRAPH_PASSWORD", "neo4j123"))

    try:
        client = Neo4jClient(uri, user, password)
    except Exception as e:
        _dbg(f"Neo4j client init failed: {e}")
        client = None

    app_name = spark.sparkContext.appName or "spark-job"
    _dbg(f"Enabling lineage (python-only) for app: {app_name}")
    tracker = LineageTracker(app_name, client)

    # Attach to spark to keep reference and allow flush
    setattr(spark, "_py_lineage_tracker", tracker)

    # Patch SparkSession.table
    orig_table = spark.table

    def table_patched(name: str):
        tracker.add_source(name)
        return orig_table(name)

    spark.table = table_patched  # type: ignore

    # Patch DataFrameReader methods
    from pyspark.sql import DataFrameReader, DataFrameWriter

    def wrap_reader_method(method_name: str):
        if not hasattr(DataFrameReader, method_name):
            return
        orig = getattr(DataFrameReader, method_name)

        def _wrapped(self, *args, **kwargs):  # type: ignore
            path = None
            if args:
                path = args[0]
            elif "path" in kwargs:
                path = kwargs.get("path")
            if path:
                tracker.add_source(str(path))
            return orig(self, *args, **kwargs)

        setattr(DataFrameReader, method_name, _wrapped)

    for m in ("csv", "parquet", "json", "orc", "text", "load"):
        wrap_reader_method(m)

    # Patch DataFrameWriter for table and file writes
    def wrap_writer_table(method_name: str):
        if not hasattr(DataFrameWriter, method_name):
            return
        orig = getattr(DataFrameWriter, method_name)

        def _wrapped(self, name: str, *args, **kwargs):  # type: ignore
            dests = [str(name)] if name else []
            tracker.emit_writes(dests, op='INSERT')
            tracker.reset()
            return orig(self, name, *args, **kwargs)

        setattr(DataFrameWriter, method_name, _wrapped)

    for m in ("saveAsTable", "insertInto"):
        wrap_writer_table(m)

    if hasattr(DataFrameWriter, "save"):
        orig_save = getattr(DataFrameWriter, "save")

        def save_patched(self, path=None, *args, **kwargs):  # type: ignore
            dests = [str(path)] if path else []
            tracker.emit_writes(dests, op='SAVE', managed=False, location=str(path) if path else None)
            tracker.reset()
            return orig_save(self, path, *args, **kwargs)

        setattr(DataFrameWriter, "save", save_patched)

    # Patch spark.sql to parse INSERT/CREATE and CTEs
    orig_sql = spark.sql

    def sql_patched(query: str, *args, **kwargs):  # type: ignore
        q = query or ""
        # Parse CTEs
        ctes = _parse_ctes(q)
        for cte_name, srcs in ctes.items():
            tracker.add_cte(cte_name, srcs)

        # Sources from FROM/JOIN at top-level
        low = re.sub(r'/\*.*?\*/', '', re.sub(r'--[^\n]*', '', q), flags=re.DOTALL).lower()
        top_sources = set(re.findall(r"\bfrom\s+([\w\.]+)", low))
        top_sources.update(re.findall(r"\bjoin\s+([\w\.]+)", low))
        for s in top_sources:
            if s not in ctes.keys():
                tracker.add_source(s)

        # Detect INSERT target
        dests: List[str] = []
        m_ins = re.search(r"insert\s+(overwrite\s+table|into)\s+([\w\.]+)", low)
        if m_ins:
            dests = [m_ins.group(2)]
            _dbg(f"SQL INSERT detected. dests={dests}")
            tracker.emit_writes(dests, op='INSERT')
            tracker.reset()
            return orig_sql(query, *args, **kwargs)

        # Detect CREATE/REPLACE/CTAS
        op, table, managed, location = _parse_create_table(q)
        if table:
            _dbg(f"SQL {op or 'CREATE'} TABLE detected. table={table}, managed={managed}, location={location}")
            tracker.emit_writes([table], op=op or 'CREATE', managed=managed, location=location)
            tracker.reset()
            return orig_sql(query, *args, **kwargs)

        # For read-only queries, just proceed (sources will flush on next write or end)
        return orig_sql(query, *args, **kwargs)

    spark.sql = sql_patched  # type: ignore

    _dbg("neo4j_lineage enabled (python-only)")


