#!/usr/bin/env python3
"""
deploy.py

Improvements:
- dry-run mode for CI preflight
- basic dependency preflight: ensure referenced stages/tables exist before creating pipes/streams
- collects created objects from SQL files and checks dependencies against them and existing schema
- strict failures (raises on first error)
- logs deployment metadata into PIPELINE_DEPLOYMENT_LOG
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
    return snowflake.connector.connect(
        user=user, password=password, account=account,
        warehouse=warehouse, database=database, schema=schema, role=role
    )

# --- Audit / Logs ---------------------------------------------------
def ensure_deploy_log(conn):
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS PIPELINE_DEPLOYMENT_LOG (
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

def ensure_migration_log(conn):
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS MIGRATION_LOG (
                SCRIPT_NAME STRING PRIMARY KEY,
                APPLIED_AT TIMESTAMP_LTZ
            )
        """)
        conn.commit()
    finally:
        cur.close()

def log_deploy(conn, deploy_id, branch, commit_sha, start_ts, end_ts, status, msg):
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO PIPELINE_DEPLOYMENT_LOG 
            (DEPLOY_ID, BRANCH, COMMIT_SHA, START_TS, END_TS, STATUS, LOG_MESSAGE)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (deploy_id, branch, commit_sha, start_ts, end_ts, status, msg))
        conn.commit()
    finally:
        cur.close()

# --- Object discovery & simple dependency parser --------------------
# These regexes are simple but cover common SQL statements used in migrations.
RE_CREATE_TABLE = re.compile(r'create\s+(?:or\s+replace\s+)?table\s+([^\s(]+)', re.IGNORECASE)
RE_CREATE_STAGE = re.compile(r'create\s+(?:or\s+replace\s+)?stage\s+([^\s;]+)', re.IGNORECASE)
RE_COPY_FROM_STAGE = re.compile(r'copy\s+into\s+[^\s]+\s+from\s+@([^\s;]+)', re.IGNORECASE)
RE_CREATE_PIPE = re.compile(r'create\s+(?:or\s+replace\s+)?pipe\s+([^\s]+)\s+as', re.IGNORECASE)
RE_STREAM_ON_TABLE = re.compile(r'create\s+(?:or\s+replace\s+)?stream\s+[^\s]+\s+on\s+table\s+([^\s;]+)', re.IGNORECASE)
RE_USE_SCHEMA = re.compile(r'use\s+schema\s+([^\s;]+)', re.IGNORECASE)

def discover_objects_in_sql(sql_text):
    """Return dict with sets: tables, stages, pipes, streams, copy_from_stages (referenced)."""
    tables = set(m.group(1).strip() for m in RE_CREATE_TABLE.finditer(sql_text))
    stages = set(m.group(1).strip() for m in RE_CREATE_STAGE.finditer(sql_text))
    pipes = set(m.group(1).strip() for m in RE_CREATE_PIPE.finditer(sql_text))
    streams = set(m.group(1).strip() for m in RE_STREAM_ON_TABLE.finditer(sql_text))
    copy_froms = set(m.group(1).strip() for m in RE_COPY_FROM_STAGE.finditer(sql_text))
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
            tables.add(r[0])

        # Stages (INFORMATION_SCHEMA.STAGES)
        cur.execute("""
            SELECT STAGE_NAME FROM INFORMATION_SCHEMA.STAGES
            WHERE STAGE_SCHEMA = %s
        """, (schema.upper(),))
        for r in cur:
            stages.add(r[0])

        return {"tables": tables, "stages": stages}
    finally:
        cur.close()

# --- Runner functions ------------------------------------------------
def run_sql_files(conn, schema, sql_glob="sql/*.sql", dry=False):
    ensure_migration_log(conn)
    cur = conn.cursor()
    try:
        # Ensure we explicitly use the database and schema
        db_name = conn.database
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
            obj = discover_objects_in_sql(sql_text)
            # store uppercase names for comparison
            declared_tables.update([t.upper() for t in obj["tables"]])
            declared_stages.update([s.upper() for s in obj["stages"]])
            declared_copy_froms.update([c.upper() for c in obj["copy_froms"]])

        logging.info("Declared tables in files: %s", sorted(declared_tables))
        logging.info("Declared stages in files: %s", sorted(declared_stages))
        logging.info("Declared copy-from targets referenced: %s", sorted(declared_copy_froms))

        # iterate files and execute (skip those already in MIGRATION_LOG)
        for path in sql_files:
            file_name = os.path.basename(path)
            cur.execute("SELECT 1 FROM MIGRATION_LOG WHERE SCRIPT_NAME=%s", (file_name,))
            if cur.fetchone():
                logging.info("Skipping already applied migration: %s", file_name)
                continue

            logging.info("Pre-checking SQL file: %s", file_name)
            with open(path, "r", encoding="utf-8") as fh:
                sql_text = fh.read()

            # Basic dependency checks (before executing):
            # 1) If file references @<stage> or @%<table> in COPY INTO or pipe, ensure referenced object exists or will be created.
            for m in RE_COPY_FROM_STAGE.finditer(sql_text):
                ref = m.group(1).strip()
                ref_up = ref.upper()
                # if reference starts with % (table stage) like %EMPLOYEE
                if ref_up.startswith('%'):
                    # table-stage; check that table exists or is declared
                    tbl = ref_up.lstrip('%')
                    tbl_exists = (tbl in existing_tables) or (tbl in declared_tables)
                    if not tbl_exists:
                        raise RuntimeError(f"Preflight check failed: COPY/PIPE references table-stage @{ref} but table '{tbl}' not found in schema or migrations.")
                else:
                    # normal stage name; check existing stages or declared stages
                    # stage names may be qualified like schema.stage; keep only name part
                    stage_name = ref_up.split('.')[-1]
                    if (stage_name not in existing_stages) and (stage_name not in declared_stages):
                        raise RuntimeError(f"Preflight check failed: COPY/PIPE references stage @{ref} but stage not found in schema or migrations.")

            # 2) If creating a stream, ensure the underlying table either exists or is declared in our files
            for m in RE_STREAM_ON_TABLE.finditer(sql_text):
                tbl = m.group(1).strip().upper()
                if (tbl not in existing_tables) and (tbl not in declared_tables):
                    raise RuntimeError(f"Preflight check failed: STREAM references table '{tbl}' but table not found in schema or migrations.")

            # If we reach here in dry-run, do not execute; just report intentions
            if dry:
                logging.info("[DRY RUN] Would execute SQL statements from %s", file_name)
                # mark would-be-applied migrations (only in logs, not in DB)
                continue

            # Execute statements sequentially
            logging.info("Running SQL file: %s", path)
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

            # Mark migration as applied
            cur.execute("INSERT INTO MIGRATION_LOG (SCRIPT_NAME, APPLIED_AT) VALUES (%s, CURRENT_TIMESTAMP)", (file_name,))
            conn.commit()

            # After successful execution, update existing_objects so subsequent files see new objects
            # quick refresh for tables and stages declared in this file
            obj = discover_objects_in_sql(sql_text)
            existing_tables.update([t.upper() for t in obj["tables"]])
            existing_stages.update([s.upper() for s in obj["stages"]])

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
        logging.error("Missing Snowflake connection envs. Aborting.")
        sys.exit(2)

    deploy_id = f"{branch}-{args.commit_sha[:8]}-{int(time.time())}"
    start_ts = datetime.utcnow()
    conn = None

    try:
        conn = connect_sf(user, password, account, warehouse, database, target_schema, role)
        ensure_deploy_log(conn)

        # backup if requested and deploying to prod/main
        if args.backup_before and branch in ("main", "prod"):
            backup_db = f"{database}_backup_{int(time.time())}"
            clone_schema(conn, database, target_schema, backup_db, target_schema)

        # Deploy SQL first (idempotent) - supports dry-run preflight
        run_sql_files(conn, target_schema, dry=args.dry_run)

        # Deploy Python scripts (optional)
        run_python_scripts(dry=args.dry_run)

        end_ts = datetime.utcnow()
        log_deploy(conn, deploy_id, branch, args.commit_sha, start_ts, end_ts, "SUCCESS", "Deployed OK (dry-run)" if args.dry_run else "Deployed OK")
        logging.info("Deployment SUCCESS")
    except Exception as e:
        end_ts = datetime.utcnow()
        try:
            if conn:
                log_deploy(conn, deploy_id, branch, args.commit_sha, start_ts, end_ts, "FAILED", str(e)[:1000])
        except Exception:
            logging.exception("Failed to write failure into deploy log")
        logging.exception("Deployment failed: %s", e)
        sys.exit(3)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
