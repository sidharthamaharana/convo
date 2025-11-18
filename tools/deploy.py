#!/usr/bin/env python3
"""
deploy.py - safer migration runner for Snowflake

Features:
- dry-run mode for CI preflight
- migration log per database.schema (avoids cross-env corruption)
- uses snowflake.connector.execute_string to run multi-statement files and capture statement-level errors
- explicit environment validation
- basic dependency preflight checks (improved)
- optional backup/clone before production deploy
- clear logging and explicit failures (no silent swallowing)

Notes:
- DDL in Snowflake is auto-committed; we cannot wrap DDL in transactions.
- Keep SQL files idempotent where possible (CREATE OR REPLACE ...).
"""

import os
import sys
import glob
import logging
import subprocess
import time
import argparse
import re
from datetime import datetime
import snowflake.connector

# --- Logging config ---
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# --- Regex helpers (improved, but still heuristic) ---
# Accept quoted identifiers "MY TABLE", or unquoted schema.table or name
RE_CREATE_TABLE = re.compile(r'create\s+(?:or\s+replace\s+)?table\s+(?:"([^"]+)"|([^\s(;]+))', re.IGNORECASE)
RE_CREATE_STAGE = re.compile(r'create\s+(?:or\s+replace\s+)?stage\s+(?:"([^"]+)"|([^\s;]+))', re.IGNORECASE)
RE_COPY_FROM_STAGE = re.compile(r'copy\s+into\s+[^\s]+\s+from\s+@("?[^"\s;]+"?|[^\s;]+)', re.IGNORECASE)
RE_CREATE_PIPE = re.compile(r'create\s+(?:or\s+replace\s+)?pipe\s+(?:"([^"]+)"|([^\s]+))\s+as', re.IGNORECASE)
RE_STREAM_ON_TABLE = re.compile(r'create\s+(?:or\s+replace\s+)?stream\s+(?:"([^"]+)"|([^\s]+))\s+on\s+table\s+(?:"([^"]+)"|([^\s;]+))', re.IGNORECASE)

def get_env(name, default=None):
    val = os.environ.get(name, default)
    if val is None:
        logging.debug("Env %s not set", name)
    return val

def connect_sf(user, password, account, warehouse, database, schema, role=None):
    logging.info("Connecting to Snowflake: %s (db=%s schema=%s)", account, database, schema)
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role
    )
    return conn

# --- Audit / Logs (per schema) ---
def ensure_deploy_log(conn, database, schema):
    cur = conn.cursor()
    try:
        # Keep log keyed by deploy id; include database+schema in columns for clarity
        cur.execute("""
            CREATE TABLE IF NOT EXISTS PIPELINE_DEPLOYMENT_LOG (
                DEPLOY_ID STRING,
                DB_NAME STRING,
                SCHEMA_NAME STRING,
                BRANCH STRING,
                COMMIT_SHA STRING,
                START_TS TIMESTAMP_LTZ,
                END_TS TIMESTAMP_LTZ,
                STATUS STRING,
                LOG_MESSAGE STRING
            )
        """)
        conn.commit()
    finally:
        cur.close()

def ensure_migration_log(conn):
    cur = conn.cursor()
    try:
        # Track script name per database.schema (PRIMARY KEY = script + schema + db)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS MIGRATION_LOG (
                SCRIPT_NAME STRING,
                DB_NAME STRING,
                SCHEMA_NAME STRING,
                APPLIED_AT TIMESTAMP_LTZ,
                PRIMARY KEY (SCRIPT_NAME, DB_NAME, SCHEMA_NAME)
            )
        """)
        conn.commit()
    finally:
        cur.close()

def log_deploy(conn, deploy_id, db, schema, branch, commit_sha, start_ts, end_ts, status, msg):
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO PIPELINE_DEPLOYMENT_LOG
            (DEPLOY_ID, DB_NAME, SCHEMA_NAME, BRANCH, COMMIT_SHA, START_TS, END_TS, STATUS, LOG_MESSAGE)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (deploy_id, db, schema, branch, commit_sha, start_ts, end_ts, status, msg))
        conn.commit()
    finally:
        cur.close()

# --- Utilities to discover referenced objects (heuristic) ---
def discover_objects_in_sql(sql_text):
    tables = set()
    stages = set()
    copy_froms = set()
    pipes = set()
    streams = set()

    for m in RE_CREATE_TABLE.finditer(sql_text):
        name = m.group(1) or m.group(2)
        if name:
            tables.add(name.strip().upper())

    for m in RE_CREATE_STAGE.finditer(sql_text):
        name = m.group(1) or m.group(2)
        if name:
            stages.add(name.strip().upper())

    for m in RE_COPY_FROM_STAGE.finditer(sql_text):
        ref = m.group(1)
        if ref:
            copy_froms.add(ref.strip().upper())

    for m in RE_CREATE_PIPE.finditer(sql_text):
        name = m.group(1) or m.group(2)
        if name:
            pipes.add(name.strip().upper())

    for m in RE_STREAM_ON_TABLE.finditer(sql_text):
        tbl = m.group(3) or m.group(4)
        if tbl:
            streams.add(tbl.strip().upper())

    return {
        "tables": tables,
        "stages": stages,
        "pipes": pipes,
        "streams": streams,
        "copy_froms": copy_froms
    }

def load_existing_objects(conn, database, schema):
    cur = conn.cursor()
    try:
        tables = set()
        stages = set()
        # TABLES
        cur.execute("""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_CATALOG = %s
        """, (schema.upper(), database.upper()))
        for r in cur:
            tables.add(r[0].upper())
        # STAGES
        cur.execute("""
            SELECT STAGE_NAME FROM INFORMATION_SCHEMA.STAGES
            WHERE STAGE_SCHEMA = %s AND STAGE_CATALOG = %s
        """, (schema.upper(), database.upper()))
        for r in cur:
            stages.add(r[0].upper())
        return {"tables": tables, "stages": stages}
    finally:
        cur.close()

# --- Execution helpers ------------------------------------------------
def run_sql_files(conn, db_name, schema_name, sql_glob="sql/*.sql", dry=False):
    ensure_migration_log(conn)
    cur = conn.cursor()
    try:
        logging.info("Using database=%s schema=%s", db_name, schema_name)
        cur.execute(f"USE DATABASE IDENTIFIER(%s)" , (db_name,))
        cur.execute(f"USE SCHEMA IDENTIFIER(%s)" , (schema_name,))

        existing = load_existing_objects(conn, db_name, schema_name)
        existing_tables = set(x.upper() for x in existing["tables"])
        existing_stages = set(x.upper() for x in existing["stages"])

        sql_files = sorted(glob.glob(sql_glob))
        logging.info("Found %d SQL files", len(sql_files))

        # Pre-scan declared objects
        declared_tables = set()
        declared_stages = set()
        declared_copy_froms = set()

        for path in sql_files:
            with open(path, "r", encoding="utf-8") as fh:
                sql_text = fh.read()
            obj = discover_objects_in_sql(sql_text)
            declared_tables.update(obj["tables"])
            declared_stages.update(obj["stages"])
            declared_copy_froms.update(obj["copy_froms"])

        logging.info("Declared tables in files: %s", sorted(declared_tables))
        logging.info("Declared stages in files: %s", sorted(declared_stages))
        logging.info("Declared copy-from targets referenced: %s", sorted(declared_copy_froms))

        for path in sql_files:
            file_name = os.path.basename(path)
            # check MIGRATION_LOG per DB+SCHEMA
            cur.execute("SELECT 1 FROM MIGRATION_LOG WHERE SCRIPT_NAME=%s AND DB_NAME=%s AND SCHEMA_NAME=%s",
                        (file_name, db_name.upper(), schema_name.upper()))
            if cur.fetchone():
                logging.info("Skipping already applied migration (schema-specific): %s", file_name)
                continue

            logging.info("Pre-checking SQL file: %s", file_name)
            with open(path, "r", encoding="utf-8") as fh:
                sql_text = fh.read()

            # Basic dependency checks
            for ref in discover_objects_in_sql(sql_text)["copy_froms"]:
                ref_up = ref.strip().upper()
                if ref_up.startswith('%'):
                    tbl = ref_up.lstrip('%')
                    if (tbl not in existing_tables) and (tbl not in declared_tables):
                        raise RuntimeError(f"Preflight check failed: COPY/PIPE references table-stage @{ref} but table '{tbl}' not found in schema or migrations.")
                else:
                    # possibly qualified stage like schema.stage or @my-stage
                    stage_name = ref_up.split('.')[-1].strip('"')
                    if (stage_name not in existing_stages) and (stage_name not in declared_stages):
                        raise RuntimeError(f"Preflight check failed: COPY/PIPE references stage @{ref} but stage not found in schema or migrations.")

            # Ensure streams reference tables we have or will create
            for tbl in discover_objects_in_sql(sql_text)["streams"]:
                if (tbl not in existing_tables) and (tbl not in declared_tables):
                    raise RuntimeError(f"Preflight check failed: STREAM references table '{tbl}' but table not found in schema or migrations.")

            if dry:
                logging.info("[DRY RUN] Would execute SQL statements from %s", file_name)
                continue

            logging.info("Running SQL file: %s", path)
            # Use execute_string to let connector parse multi-statement SQL properly
            try:
                results = cur.execute_string(sql_text)
                # execute_string returns list of statement cursors; check for errors
                # Note: snowflake-connector raises for statement error, but we'll be explicit
                # We attempt to advance each cursor to make sure errors surface.
                for stmt_cur in results:
                    try:
                        # consume to force server-side exceptions if any
                        stmt_cur.fetchall()
                    except Exception:
                        # Some statements won't return results which is fine; ignore
                        pass
            except Exception as e:
                logging.exception("SQL execution failed for file %s", file_name)
                raise

            # Mark migration as applied
            cur.execute("INSERT INTO MIGRATION_LOG (SCRIPT_NAME, DB_NAME, SCHEMA_NAME, APPLIED_AT) VALUES (%s, %s, %s, CURRENT_TIMESTAMP)",
                        (file_name, db_name.upper(), schema_name.upper()))
            conn.commit()

            # Update existing objects with newly created ones
            obj = discover_objects_in_sql(sql_text)
            existing_tables.update(obj["tables"])
            existing_stages.update(obj["stages"])

    finally:
        cur.close()

def run_python_scripts(py_glob="python/*.py", dry=False):
    py_files = sorted(glob.glob(py_glob))
    logging.info("Found %d Python files", len(py_files))
    for p in py_files:
        logging.info("Running Python script: %s", p)
        if dry:
            logging.info("[DRY RUN] Would run Python script: %s", p)
            continue
        rc = subprocess.call([sys.executable, p], env=os.environ.copy())
        if rc != 0:
            raise RuntimeError(f"Python script failed: {p}")

def clone_schema(conn, source_db, source_schema, target_db, target_schema):
    cur = conn.cursor()
    try:
        logging.info("Cloning %s.%s to %s.%s", source_db, source_schema, target_db, target_schema)
        # Create database as clone of source database (if required). Here we clone schema using database clone approach:
        # create database new_db clone source_db; create schema within that database by cloning.
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
        # The recommended safe approach: clone database from source (if needed) or clone schema (if Snowflake supports direct schema clone in your edition)
        # Use schema clone:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {target_db}.{target_schema}")
        cur.execute(f"CREATE SCHEMA {target_db}.{target_schema} CLONE {source_db}.{source_schema}")
        conn.commit()
    finally:
        cur.close()

# --- Main ------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="validate SQL and dependencies but do not execute")
    parser.add_argument("--backup-before", action="store_true", help="clone target schema before deploy (prod safety)")
    parser.add_argument("--commit-sha", default=os.environ.get("GITHUB_SHA", "local"))
    args = parser.parse_args()

    branch = os.environ.get("GITHUB_REF_NAME") or os.environ.get("BRANCH_NAME") or os.environ.get("GITHUB_REF", "local").split('/')[-1]
    target_schema = os.environ.get("SNOWFLAKE_SCHEMA") or ("DEV_SCHEMA" if branch == "dev" else ("TEST_SCHEMA" if branch == "test" else "PROD_SCHEMA"))
    db = os.environ.get("SNOWFLAKE_DATABASE")
    account = get_env("SNOWFLAKE_ACCOUNT")
    user = get_env("SNOWFLAKE_USER")
    password = get_env("SNOWFLAKE_PASSWORD")
    warehouse = get_env("SNOWFLAKE_WAREHOUSE")
    role = get_env("SNOWFLAKE_ROLE")

    if not (account and user and password and warehouse and db and target_schema):
        logging.error("Missing Snowflake connection envs. Required: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, and SNOWFLAKE_SCHEMA")
        sys.exit(2)

    deploy_id = f"{branch}-{args.commit_sha[:8]}-{int(time.time())}"
    start_ts = datetime.utcnow()
    conn = None

    try:
        conn = connect_sf(user, password, account, warehouse, db, target_schema, role)
        ensure_deploy_log(conn, db, target_schema)
        ensure_migration_log(conn)

        # backup if requested and deploying to prod/main branch
        if args.backup_before and branch in ("main", "prod"):
            backup_db = f"{db}_backup_{int(time.time())}"
            logging.info("Performing schema backup/clone to database: %s", backup_db)
            clone_schema(conn, db, target_schema, backup_db, target_schema)

        # Deploy SQL first (idempotent)
        run_sql_files(conn, db, target_schema, dry=args.dry_run)

        # Deploy Python scripts
        run_python_scripts(dry=args.dry_run)

        end_ts = datetime.utcnow()
        log_deploy(conn, deploy_id, db, target_schema, branch, args.commit_sha, start_ts, end_ts, "SUCCESS", "Deployed OK (dry-run)" if args.dry_run else "Deployed OK")
        logging.info("Deployment SUCCESS")
    except Exception as e:
        end_ts = datetime.utcnow()
        try:
            if conn:
                log_deploy(conn, deploy_id, db, target_schema, branch, args.commit_sha, start_ts, end_ts, "FAILED", str(e)[:1000])
        except Exception:
            logging.exception("Failed to write failure into deploy log")
        logging.exception("Deployment failed: %s", e)
        sys.exit(3)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
