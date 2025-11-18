#!/usr/bin/env python3
"""
deploy.py - improved & hardened migration runner

Key fixes:
- Migration & deploy logs are created/queried using fully-qualified names:
  <DATABASE>.<SCHEMA>.MIGRATION_LOG and <DATABASE>.<SCHEMA>.PIPELINE_DEPLOYMENT_LOG
  so metadata persists reliably per target schema.
- SCRIPT_NAME is stored uppercased and compared case-insensitively.
- Detects 'USE SCHEMA' statements in SQL files and fails early to avoid session schema hops.
- Dry-run still reports intent without executing.
- Fails fast on SQL / Python script errors.
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

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# --- Helpers ---------------------------------------------------------
def get_env(name, default=None):
    val = os.environ.get(name, default)
    if val is None:
        logging.debug("Env %s not set", name)
    return val

def connect_sf(user, password, account, warehouse, database, schema, role=None):
    logging.info("Connecting to Snowflake: %s (db=%s schema=%s)", account, database, schema)
    # pass database and schema as connection parameters so session defaults are set
    return snowflake.connector.connect(
        user=user, password=password, account=account,
        warehouse=warehouse, database=database, schema=schema, role=role
    )

# --- Qualified name helpers -----------------------------------------
def qualify(db, schema, name):
    """Return fully-qualified identifier DB.SCHEMA.NAME (no quoting)."""
    return f"{db}.{schema}.{name}"

# --- Audit / Logs ---------------------------------------------------
def ensure_deploy_log(conn, database, schema):
    cur = conn.cursor()
    fq = qualify(database, schema, "PIPELINE_DEPLOYMENT_LOG")
    try:
        logging.info("Ensuring deploy log exists: %s", fq)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {fq} (
                DEPLOY_ID STRING,
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

def ensure_migration_log(conn, database, schema):
    cur = conn.cursor()
    fq = qualify(database, schema, "MIGRATION_LOG")
    try:
        logging.info("Ensuring migration log exists: %s", fq)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {fq} (
                SCRIPT_NAME STRING PRIMARY KEY,
                APPLIED_AT TIMESTAMP_LTZ
            )
        """)
        conn.commit()
    finally:
        cur.close()

def log_deploy(conn, database, schema, deploy_id, branch, commit_sha, start_ts, end_ts, status, msg):
    cur = conn.cursor()
    fq = qualify(database, schema, "PIPELINE_DEPLOYMENT_LOG")
    try:
        cur.execute(f"""
            INSERT INTO {fq}
            (DEPLOY_ID, BRANCH, COMMIT_SHA, START_TS, END_TS, STATUS, LOG_MESSAGE)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (deploy_id, branch, commit_sha, start_ts, end_ts, status, msg))
        conn.commit()
    finally:
        cur.close()

# --- Object discovery & simple dependency parser --------------------
RE_CREATE_TABLE = re.compile(r'create\s+(?:or\s+replace\s+)?table\s+([^\s(;]+)', re.IGNORECASE)
RE_CREATE_STAGE = re.compile(r'create\s+(?:or\s+replace\s+)?stage\s+([^\s;]+)', re.IGNORECASE)
RE_COPY_FROM_STAGE = re.compile(r'copy\s+into\s+[^\s]+\s+from\s+@([^\s;]+)', re.IGNORECASE)
RE_CREATE_PIPE = re.compile(r'create\s+(?:or\s+replace\s+)?pipe\s+([^\s]+)\s+as', re.IGNORECASE)
RE_STREAM_ON_TABLE = re.compile(r'create\s+(?:or\s+replace\s+)?stream\s+[^\s]+\s+on\s+table\s+([^\s;]+)', re.IGNORECASE)
RE_USE_SCHEMA = re.compile(r'^\s*use\s+schema\s+([^\s;]+)', re.IGNORECASE | re.MULTILINE)

def discover_objects_in_sql(sql_text):
    """Return dict with sets: tables, stages, pipes, streams, copy_froms (uppercased, unquoted)."""
    tables = set(m.group(1).strip().upper() for m in RE_CREATE_TABLE.finditer(sql_text))
    stages = set(m.group(1).strip().upper() for m in RE_CREATE_STAGE.finditer(sql_text))
    pipes = set(m.group(1).strip().upper() for m in RE_CREATE_PIPE.finditer(sql_text))
    streams = set(m.group(1).strip().upper() for m in RE_STREAM_ON_TABLE.finditer(sql_text))
    copy_froms = set(m.group(1).strip().upper() for m in RE_COPY_FROM_STAGE.finditer(sql_text))
    return {
        "tables": tables,
        "stages": stages,
        "pipes": pipes,
        "streams": streams,
        "copy_froms": copy_froms
    }

def load_existing_objects(conn, database, schema):
    """Load existing tables and stages from INFORMATION_SCHEMA for quick checks."""
    cur = conn.cursor()
    try:
        tables = set()
        stages = set()

        # Tables (INFORMATION_SCHEMA.TABLES)
        cur.execute("""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
        """, (schema.upper(),))
        for r in cur:
            tables.add(r[0].upper())

        # Stages (INFORMATION_SCHEMA.STAGES)
        cur.execute("""
            SELECT STAGE_NAME FROM INFORMATION_SCHEMA.STAGES
            WHERE STAGE_SCHEMA = %s
        """, (schema.upper(),))
        for r in cur:
            stages.add(r[0].upper())

        return {"tables": tables, "stages": stages}
    finally:
        cur.close()

# --- Runner functions ------------------------------------------------
def run_sql_files(conn, database, schema, sql_glob="sql/*.sql", dry=False):
    ensure_migration_log(conn, database, schema)
    cur = conn.cursor()
    migration_fq = qualify(database, schema, "MIGRATION_LOG")
    try:
        # Ensure we explicitly use the database and schema
        db_name = conn.database
        if not db_name:
            db_name = database
        logging.info("Using database=%s schema=%s", db_name, schema)
        cur.execute(f"USE DATABASE {db_name}")
        cur.execute(f"USE SCHEMA {schema}")

        # Seed existing objects from schema (before applying our migrations)
        existing = load_existing_objects(conn, db_name, schema)
        existing_tables = set(x.upper() for x in existing["tables"])
        existing_stages = set(x.upper() for x in existing["stages"])

        # Collect all SQL files
        sql_files = sorted(glob.glob(sql_glob))
        logging.info("Found %d SQL files", len(sql_files))

        # Pre-scan: collect objects defined inside our SQL files
        declared_tables = set()
        declared_stages = set()
        declared_copy_froms = set()

        for path in sql_files:
            with open(path, "r", encoding="utf-8") as fh:
                sql_text = fh.read()
            # Fail fast if file explicitly issues USE SCHEMA - prevents schema drift
            if RE_USE_SCHEMA.search(sql_text):
                raise RuntimeError(f"Migration file {os.path.basename(path)} contains a 'USE SCHEMA' statement. Remove it; deploy script controls schema.")
            obj = discover_objects_in_sql(sql_text)
            declared_tables.update(obj["tables"])
            declared_stages.update(obj["stages"])
            declared_copy_froms.update(obj["copy_froms"])

        logging.info("Declared tables in files: %s", sorted(declared_tables))
        logging.info("Declared stages in files: %s", sorted(declared_stages))
        logging.info("Declared copy-from targets referenced: %s", sorted(declared_copy_froms))

        # iterate files and execute (skip those already in MIGRATION_LOG)
        for path in sql_files:
            file_name = os.path.basename(path)
            file_name_upper = file_name.upper()
            cur.execute(f"SELECT 1 FROM {migration_fq} WHERE UPPER(SCRIPT_NAME) = %s", (file_name_upper,))
            if cur.fetchone():
                logging.info("Skipping already applied migration: %s", file_name)
                continue

            logging.info("Pre-checking SQL file: %s", file_name)
            with open(path, "r", encoding="utf-8") as fh:
                sql_text = fh.read()

            # Basic dependency checks (before executing):
            for m in RE_COPY_FROM_STAGE.finditer(sql_text):
                ref = m.group(1).strip()
                ref_up = ref.upper()
                if ref_up.startswith('%'):
                    tbl = ref_up.lstrip('%')
                    tbl_exists = (tbl in existing_tables) or (tbl in declared_tables)
                    if not tbl_exists:
                        raise RuntimeError(f"Preflight check failed: COPY/PIPE references table-stage @{ref} but table '{tbl}' not found in schema or migrations.")
                else:
                    stage_name = ref_up.split('.')[-1]
                    if (stage_name not in existing_stages) and (stage_name not in declared_stages):
                        raise RuntimeError(f"Preflight check failed: COPY/PIPE references stage @{ref} but stage not found in schema or migrations.")

            for m in RE_STREAM_ON_TABLE.finditer(sql_text):
                tbl = m.group(1).strip().upper()
                # If fully qualified 'schema.table' take last part
                tbl_simple = tbl.split('.')[-1]
                if (tbl_simple not in existing_tables) and (tbl_simple not in declared_tables):
                    raise RuntimeError(f"Preflight check failed: STREAM references table '{tbl}' but table not found in schema or migrations.")

            if dry:
                logging.info("[DRY RUN] Would execute SQL statements from %s", file_name)
                continue

            # Execute statements sequentially
            logging.info("Running SQL file: %s", path)
            # naive split by ; but keeps same behavior as before
            statements = [s.strip() for s in sql_text.split(";") if s.strip()]
            for stmt in statements:
                try:
                    logging.info("Executing statement: %.120s", stmt.replace("\n"," ")[:120])
                    cur.execute(stmt)
                    # attempt to consume results to force server-side errors if any
                    try:
                        cur.fetchall()
                    except Exception:
                        pass
                except Exception as e:
                    logging.exception("SQL failed in %s: %s", path, e)
                    raise

            # Mark migration as applied (store uppercased script name to avoid case issues)
            cur.execute(f"INSERT INTO {migration_fq} (SCRIPT_NAME, APPLIED_AT) VALUES (%s, CURRENT_TIMESTAMP)", (file_name_upper,))
            conn.commit()

            # After successful execution, update existing_objects so subsequent files see new objects
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
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {target_db}.{target_schema}")
        # Use CLONE only to create target schema from source
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
    # If SNOWFLAKE_SCHEMA env exists, use it; else use branch-based fallback.
    target_schema = os.environ.get("SNOWFLAKE_SCHEMA") or (
        "DEV_SCHEMA" if branch == "dev" else ("TEST_SCHEMA" if branch == "test" else "PROD_SCHEMA")
    )

    account = get_env("SNOWFLAKE_ACCOUNT")
    user = get_env("SNOWFLAKE_USER")
    password = get_env("SNOWFLAKE_PASSWORD")
    warehouse = get_env("SNOWFLAKE_WAREHOUSE")
    database = get_env("SNOWFLAKE_DATABASE")
    role = get_env("SNOWFLAKE_ROLE")

    if not (account and user and password and warehouse and database):
        logging.error("Missing Snowflake connection envs. Aborting. Required: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE")
        sys.exit(2)

    deploy_id = f"{branch}-{args.commit_sha[:8]}-{int(time.time())}"
    start_ts = datetime.utcnow()
    conn = None

    try:
        conn = connect_sf(user, password, account, warehouse, database, target_schema, role)
        # Ensure deploy log exists in the target DB.SCHEMA (so every run uses same place)
        ensure_deploy_log(conn, database, target_schema)

        # backup if requested and deploying to prod/main
        if args.backup_before and branch in ("main", "prod"):
            backup_db = f"{database}_backup_{int(time.time())}"
            clone_schema(conn, database, target_schema, backup_db, target_schema)

        # Deploy SQL first (idempotent) - supports dry-run preflight
        run_sql_files(conn, database, target_schema, dry=args.dry_run)

        # Deploy Python scripts (optional)
        run_python_scripts(dry=args.dry_run)

        end_ts = datetime.utcnow()
        log_deploy(conn, database, target_schema, deploy_id, branch, args.commit_sha, start_ts, end_ts, "SUCCESS", "Deployed OK (dry-run)" if args.dry_run else "Deployed OK")
        logging.info("Deployment SUCCESS")
    except Exception as e:
        end_ts = datetime.utcnow()
        try:
            if conn:
                # try to record failure in deploy log; swallow if that fails
                log_deploy(conn, database, target_schema, deploy_id, branch, args.commit_sha, start_ts, end_ts, "FAILED", str(e)[:1000])
        except Exception:
            logging.exception("Failed to write failure into deploy log")
        logging.exception("Deployment failed: %s", e)
        sys.exit(3)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
