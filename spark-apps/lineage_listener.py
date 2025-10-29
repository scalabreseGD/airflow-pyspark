from __future__ import annotations

import atexit
import os
import re
from typing import List, Set, Optional

try:
    from neo4j import GraphDatabase as _Neo4jDriver
except Exception:
    _Neo4jDriver = None
from pyspark.sql import SparkSession, DataFrameReader, DataFrameWriter


class Neo4jClient:
    def __init__(self, uri: str, user: Optional[str], password: Optional[str]) -> None:
        if _Neo4jDriver is None:
            raise ImportError("neo4j driver not installed; set LINEAGE_TO_NEO4J=false or install neo4j package")
        auth = (user, password)
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

    def upsert_lineage(self, job_name: str, sources: List[str], destinations: List[str], cte_lineage: Optional[dict] = None) -> None:
        _dbg(f"Upsert lineage: job={job_name}, sources={len(sources)}, destinations={len(destinations)}, ctes={len(cte_lineage or {})}")
        cypher_sources = "UNWIND $sources AS s MERGE (:Dataset {name: s})"
        cypher_destinations = "UNWIND $destinations AS d MERGE (:Dataset {name: d})"
        cypher_edges_write = (
            "MERGE (j:SparkJob {name: $job}) "
            "WITH j "
            "UNWIND $pairs AS p "
            "MATCH (s:Dataset {name: p.source}), (d:Dataset {name: p.dest}) "
            "MERGE (s)-[:FLOWS_TO]->(j) "
            "MERGE (j)-[:WRITES_TO]->(d)"
        )
        cypher_edges_readonly = (
            "MERGE (j:SparkJob {name: $job}) "
            "WITH j "
            "UNWIND $sources AS sname "
            "MATCH (s:Dataset {name: sname}) "
            "MERGE (s)-[:FLOWS_TO]->(j)"
        )
        # CTE lineage: create CTE nodes and relationships
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
        cypher_cte_to_job = (
            "MERGE (j:SparkJob {name: $job}) "
            "WITH j "
            "UNWIND $cte_names AS cname "
            "MATCH (cte:CTE {name: cname, job: $job}) "
            "MERGE (cte)-[:FLOWS_TO]->(j)"
        )
        try:
            with self._driver.session() as session:
                src_list = list(set(sources or []))
                dst_list = list(set(destinations or []))
                if src_list:
                    _dbg(f"Merging {len(src_list)} source Dataset nodes")
                    session.run(cypher_sources, sources=src_list)
                if dst_list:
                    _dbg(f"Merging {len(dst_list)} destination Dataset nodes")
                    session.run(cypher_destinations, destinations=dst_list)

                # Handle CTE lineage
                if cte_lineage:
                    # Separate table sources from CTE dependencies
                    all_cte_names = set(cte_lineage.keys())
                    cte_list = []
                    for name, srcs in cte_lineage.items():
                        table_srcs = [s for s in srcs if s not in all_cte_names]
                        cte_deps = [s for s in srcs if s in all_cte_names]
                        cte_list.append({
                            "name": name,
                            "sources": table_srcs,
                            "cte_deps": cte_deps
                        })

                    _dbg(f"Creating {len(cte_list)} CTE nodes")
                    session.run(cypher_cte_nodes, ctes=cte_list, job=job_name)

                    # Create edges from tables to CTEs
                    _dbg(f"Creating CTE source edges")
                    session.run(cypher_cte_edges, ctes=cte_list, job=job_name)

                    # Create edges from CTEs to other CTEs
                    cte_with_deps = [c for c in cte_list if c["cte_deps"]]
                    if cte_with_deps:
                        _dbg(f"Creating CTE-to-CTE edges")
                        session.run(cypher_cte_to_cte, ctes=cte_with_deps, job=job_name)

                    _dbg(f"Creating CTE to job edges")
                    session.run(cypher_cte_to_job, cte_names=list(cte_lineage.keys()), job=job_name)

                if dst_list:
                    pairs = [{"source": s, "dest": d} for s in set(src_list) for d in set(dst_list)]
                    if pairs:
                        _dbg(f"Creating write edges for {len(pairs)} source-dest pairs")
                        session.run(cypher_edges_write, pairs=pairs, job=job_name)
                elif src_list:
                    _dbg(f"Creating read-only edges for {len(src_list)} sources")
                    session.run(cypher_edges_readonly, sources=src_list, job=job_name)
        except Exception as e:
            _dbg(f"ERROR writing lineage to Neo4j: {e}")


class _LineageTracker:
    def __init__(self, job_name: str, client: Neo4jClient | None) -> None:
        self.job_name = job_name
        self.client = client
        self.sources: Set[str] = set()
        self.cte_lineage: dict = {}

    def add_source(self, name: str) -> None:
        if not name:
            return
        self.sources.add(name)
        _dbg(f"Source added: {name}")

    def add_cte_lineage(self, cte_name: str, sources: Set[str]) -> None:
        if not cte_name:
            return
        self.cte_lineage[cte_name] = sources
        _dbg(f"CTE lineage added: {cte_name} -> {sources}")

    def emit(self, destinations: List[str]) -> None:
        if not self.client:
            _dbg("Skipping emit: no Neo4j client (init failed or disabled)")
            return
        try:
            _dbg(f"Emitting lineage: sources={list(self.sources)}, destinations={destinations}, ctes={list(self.cte_lineage.keys())}")
            self.client.upsert_lineage(self.job_name, list(self.sources), destinations, self.cte_lineage)
        finally:
            # Reset sources and CTEs after a write boundary
            self.sources.clear()
            self.cte_lineage.clear()


def _extract_cte_sources(sql: str) -> dict:
    """
    Extract CTE definitions and their sources from SQL.
    Returns dict: {cte_name: set(source_tables)}
    """
    cte_lineage = {}

    # Pattern to match: WITH cte_name AS ( or , cte_name AS (
    # This catches both the first CTE and subsequent CTEs separated by commas
    cte_pattern = r'(?:\bwith\s+|,\s*)(\w+)\s+as\s*\('

    for match in re.finditer(cte_pattern, sql, re.IGNORECASE):
        cte_name = match.group(1).lower()
        start_pos = match.end() - 1  # Position of opening paren

        # Find matching closing paren
        paren_count = 1
        pos = start_pos + 1
        end_pos = start_pos

        while pos < len(sql) and paren_count > 0:
            if sql[pos] == '(':
                paren_count += 1
            elif sql[pos] == ')':
                paren_count -= 1
                if paren_count == 0:
                    end_pos = pos
                    break
            pos += 1

        if end_pos > start_pos:
            # Extract CTE body
            cte_body = sql[start_pos + 1:end_pos]

            # Extract FROM and JOIN references from CTE body
            cte_sources = set()
            from_matches = re.findall(r'\bfrom\s+([\w\.]+)', cte_body, re.IGNORECASE)
            join_matches = re.findall(r'\bjoin\s+([\w\.]+)', cte_body, re.IGNORECASE)

            cte_sources.update(m.lower() for m in from_matches)
            cte_sources.update(m.lower() for m in join_matches)

            # Filter out other CTEs (will be resolved as dependencies)
            # Store the sources for this CTE
            if cte_sources:
                cte_lineage[cte_name] = cte_sources
                _dbg(f"Extracted CTE '{cte_name}' with sources: {cte_sources}")

    return cte_lineage


def _enable_monkeypatch_lineage(spark: SparkSession) -> None:
    enabled = os.getenv("LINEAGE_TO_NEO4J", "true").lower() in ("1", "true", "yes")
    if not enabled:
        _dbg("Lineage disabled via LINEAGE_TO_NEO4J env var")
        return

    uri = os.getenv("NEO4J_URI", os.getenv("NEO4J_URI", "bolt://neo4j:7687"))
    user = os.getenv("NEO4J_USER", os.getenv("NEO4J_USER", "neo4j"))
    password = os.getenv("NEO4J_PASSWORD", os.getenv("NEO4J_PASSWORD", "neo4j123"))

    try:
        client = Neo4jClient(uri, user, password)
    except Exception as e:
        _dbg(f"Neo4j client init failed: {e}")
        client = None

    app_name = spark.sparkContext.appName or "spark-job"
    _dbg(f"Enabling lineage for app: {app_name}")
    tracker = _LineageTracker(app_name, client)

    # Attach tracker to session to avoid GC
    setattr(spark, "_lineage_tracker", tracker)

    # Patch SparkSession.table
    orig_table = spark.table

    def table_patched(name: str):
        tracker.add_source(name)
        return orig_table(name)

    spark.table = table_patched  # type: ignore[assignment]

    # Patch DataFrameReader methods
    def wrap_reader_method(method_name: str):
        orig = getattr(DataFrameReader, method_name)

        def _wrapped(self, *args, **kwargs):  # type: ignore[no-redef]
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
        if hasattr(DataFrameReader, m):
            wrap_reader_method(m)

    # Patch DataFrameWriter methods
    def wrap_writer_dest_table(method_name: str):
        orig = getattr(DataFrameWriter, method_name)

        def _wrapped(self, name: str, *args, **kwargs):  # type: ignore[no-redef]
            dests = [str(name)] if name else []
            tracker.emit(dests)
            return orig(self, name, *args, **kwargs)

        setattr(DataFrameWriter, method_name, _wrapped)

    for m in ("saveAsTable", "insertInto"):
        if hasattr(DataFrameWriter, m):
            wrap_writer_dest_table(m)

    # Patch DataFrameWriter.save (file path destinations)
    if hasattr(DataFrameWriter, "save"):
        orig_save = getattr(DataFrameWriter, "save")

        def save_patched(self, path=None, *args, **kwargs):  # type: ignore[no-redef]
            dests = [str(path)] if path else []
            tracker.emit(dests)
            return orig_save(self, path, *args, **kwargs)

        setattr(DataFrameWriter, "save", save_patched)

    # Patch spark.sql to handle INSERT ... SELECT lineage
    orig_sql = spark.sql

    def sql_patched(query: str, *args, **kwargs):
        q = query or ""
        # Strip SQL comments before parsing to avoid false matches
        # Remove single-line comments (-- ...)
        q_no_comments = re.sub(r'--[^\n]*', '', q)
        # Remove multi-line comments (/* ... */)
        q_no_comments = re.sub(r'/\*.*?\*/', '', q_no_comments, flags=re.DOTALL)
        lower = q_no_comments.lower()

        # Extract destination table for INSERT INTO/OVERWRITE TABLE dest ...
        dests: List[str] = []

        # Extract CTE definitions and their source tables
        cte_lineage = _extract_cte_sources(q_no_comments)
        cte_names = set(cte_lineage.keys())

        # Add CTE lineage to tracker (keep all sources including other CTEs)
        for cte_name, cte_sources in cte_lineage.items():
            if cte_sources:
                tracker.add_cte_lineage(cte_name, cte_sources)

        # Extract simple FROM and JOIN table references for any SQL
        sources = set(re.findall(r"\bfrom\s+([\w\.]+)", lower))
        sources.update(re.findall(r"\bjoin\s+([\w\.]+)", lower))

        # Filter out CTE names from sources (they're not real tables)
        # but add the actual tables used by CTEs
        real_sources = sources - cte_names

        for s in real_sources:
            tracker.add_source(s)

        if lower.strip().startswith("insert"):
            m = re.search(r"insert\s+(overwrite\s+table|into)\s+([\w\.]+)", lower)
            if m:
                dests = [m.group(2)]
            if dests:
                _dbg(f"SQL INSERT detected. dests={dests}, sources={list(real_sources)}, ctes={list(cte_names)}")
                tracker.emit(dests)
        elif real_sources:
            _dbg(f"SQL SELECT/other detected. sources={list(real_sources)}, ctes={list(cte_names)}")
        return orig_sql(query, *args, **kwargs)

    spark.sql = sql_patched  # type: ignore[assignment]
    _dbg("Lineage monkeypatches applied")

    # Emit any collected sources on interpreter exit (read-only jobs)
    def _flush_on_exit():
        if getattr(spark, "_lineage_tracker", None) and spark._lineage_tracker.sources:
            _dbg("Process exit flush: emitting read-only lineage")
            try:
                spark._lineage_tracker.emit([])
            except Exception as e:
                _dbg(f"Flush error: {e}")

    atexit.register(_flush_on_exit)


def enable_lineage(spark: SparkSession) -> None:
    _enable_monkeypatch_lineage(spark)


def register_lineage_listener(spark: SparkSession) -> None:
    # Backward-compatible API name used in scripts
    enable_lineage(spark)


def _dbg(msg: str) -> None:
    try:
        if os.getenv("LINEAGE_DEBUG", "true").lower() in ("1", "true", "yes"):
            print(f"[LINEAGE] {msg}")
    except Exception:
        pass
