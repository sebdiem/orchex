from __future__ import annotations


def quote_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def qualify(schema: str, table: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(table)}"


__all__ = ["quote_ident", "qualify"]
