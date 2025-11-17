#!/usr/bin/env python3
import os, sys, glob, logging, subprocess, time, argparse
from datetime import datetime
import snowflake.connector

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


def get_env(name, default=None):
    val = os.environ.get(name, default)
    if val is None:
        logging.debug("Env %s not set", name)
    return val


def connect_sf(user, password, account, warehouse, database, schema, role=None):
    logging.info("Connecting to Snowflake: %s (db=%s schema=%s)", account, database, schema)
    conn = snowflake.connector.connect(
        user=user, password=password, account=account,
        warehouse=warehouse, database=database, schema=schema, role=role
    )
    return conn


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
        INSERT INTO PIPELINE_DEPLOYMENT_LOG (DEPLOY_ID, BRANCH, COMMIT_SHA, START_TS, END_TS, STATUS, LOG_MESSAGE)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (deploy_id, branch, commit_sha, start_ts, end_ts, status, msg))
        conn.commit()
    finally:
        cur.close()


def run_sql_files(conn, schema, sql_glob="sql/*.sql", dry=False):
    ensure_migration_log(conn)
    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {conn.database}")
        cur.execute(f"USE SCHEMA {schema}")
        sql_files = sorted(glob.glob(sql_glob))
        logging.info("Found %d SQL files", len(sql_files))

        for path in sql_files:
            file_name = os.path.basename(path)
            # Check if this migration already applied
            cur.execute("SELECT 1 FROM MIGRATION_LOG WHERE SCRIPT_NAME=%s", (file_name,))
            if cur.fetchone():
                logging.info("Skipping already applied migration: %s", file_name)
                continue

            logging.info("Running SQL file: %s", path)
            with open(path, "r", encoding="utf-8") as fh:
                sql_text = fh.read()

            statements = [s.strip() for s in sql_text.split(";") if s.strip()]
            for stmt in statements:
                if dry:
                    logging.info("[DRY RUN] Would execute: %s", stmt[:80])
                    continue
                try:
                    logging.info("Executing statement: %s", stmt[:120])
                    cur.execute(stmt)
                    try:
                        cur.fetchall()
                    except:
                        pass
                except Exception as e:
                    logging.exception("SQL failed in %s: %s", path, e)
                    raise

            # Mark migration as applied
            cur.execute("INSERT INTO MIGRATION_LOG (SCRIPT_NAME, APPLIED_AT) VALUES (%s, CURRENT_TIMESTAMP)", (file_name,))
            conn.commit()

    finally:
        cur.close()


def run_python_scripts(py_glob="python/*.py"):
    py_files = sorted(glob.glob(py_glob))
    logging.info("Found %d Python files", len(py_files))
    for p in py_files:
        logging.info("Running Python script: %s", p)
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
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
        if args.backup_before and branch in ("main","prod"):
            backup_db = f"{database}_backup_{int(time.time())}"
            clone_schema(conn, database, target_schema, backup_db, target_schema)

        # Deploy SQL first (idempotent)
        run_sql_files(conn, target_schema, dry=args.dry_run)
        # Deploy Python scripts (optional, careful with reruns)
        run_python_scripts()

        end_ts = datetime.utcnow()
        log_deploy(conn, deploy_id, branch, args.commit_sha, start_ts, end_ts, "SUCCESS", "Deployed OK")
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
