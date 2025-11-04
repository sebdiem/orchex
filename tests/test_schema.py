from __future__ import annotations

from orchex.schema import render_schema_sql


def test_render_schema_sql_quotes_schema_name():
    sql = render_schema_sql("analytics")
    assert 'CREATE SCHEMA IF NOT EXISTS "analytics";' in sql
    assert "dag_name    TEXT NOT NULL" in sql


def test_render_schema_sql_qualifies_tables():
    sql = render_schema_sql("observability")
    assert 'CREATE TABLE IF NOT EXISTS "observability"."settings"' in sql
    assert 'CREATE TABLE IF NOT EXISTS "observability"."dag_snapshots"' in sql
    assert 'CREATE INDEX IF NOT EXISTS "observability_idx_snapshots_dag"' in sql
