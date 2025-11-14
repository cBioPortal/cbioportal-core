#!/usr/bin/env python3

"""Convert MySQL cgds schema/seed SQL into ClickHouse-compatible statements."""

from __future__ import annotations

import argparse
import datetime as dt
import pathlib
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional


TYPE_MAP = [
    (re.compile(r"bigint\(\d+\)\s+unsigned", re.I), "UInt64"),
    (re.compile(r"bigint\(\d+\)", re.I), "Int64"),
    (re.compile(r"int\(\d+\)\s+unsigned", re.I), "UInt32"),
    (re.compile(r"int\(\d+\)", re.I), "Int32"),
    (re.compile(r"mediumint\(\d+\)", re.I), "Int32"),
    (re.compile(r"smallint\(\d+\)\s+unsigned", re.I), "UInt16"),
    (re.compile(r"smallint\(\d+\)", re.I), "Int16"),
    (re.compile(r"tinyint\(\d+\)\s+unsigned", re.I), "UInt8"),
    (re.compile(r"tinyint\(1\)", re.I), "Int8"),
    (re.compile(r"tinyint\(\d+\)", re.I), "Int8"),
    (re.compile(r"double", re.I), "Float64"),
    (re.compile(r"float", re.I), "Float32"),
    (re.compile(r"decimal\((\d+,\d+)\)", re.I), lambda m: f"Decimal({m.group(1)})"),
    (re.compile(r"varchar\(\d+\)", re.I), "String"),
    (re.compile(r"char\(\d+\)", re.I), "String"),
    (re.compile(r"longtext", re.I), "String"),
    (re.compile(r"mediumtext", re.I), "String"),
    (re.compile(r"text", re.I), "String"),
    (re.compile(r"blob", re.I), "String"),
    (re.compile(r"longblob", re.I), "String"),
    (re.compile(r"tinyblob", re.I), "String"),
    (re.compile(r"enum\(", re.I), "String"),
    (re.compile(r"set\(", re.I), "String"),
    (re.compile(r"datetime", re.I), "DateTime"),
    (re.compile(r"timestamp", re.I), "DateTime"),
    (re.compile(r"date", re.I), "Date"),
    (re.compile(r"time", re.I), "String"),
    (re.compile(r"binary\(\d+\)", re.I), "String"),
    (re.compile(r"varbinary\(\d+\)", re.I), "String"),
]

ADDITIONAL_CLICKHOUSE_TABLES = {
    "cosmic_mutation": """DROP TABLE IF EXISTS `cosmic_mutation`;
CREATE TABLE IF NOT EXISTS `cosmic_mutation` (
  `cosmic_mutation_id` String,
  `chr` String,
  `start_position` Int64,
  `reference_allele` String,
  `tumor_seq_allele` String,
  `strand` String,
  `codon_change` String,
  `entrez_gene_id` Int32,
  `protein_change` String,
  `count` Int32,
  `keyword` String
) ENGINE = MergeTree
ORDER BY (`cosmic_mutation_id`);
"""
}


@dataclass
class ColumnDef:
    name: str
    mysql_type: str
    clickhouse_type: str
    nullable: bool
    default: Optional[str]

    def render(self) -> str:
        dtype = self.clickhouse_type
        if self.nullable and not dtype.startswith("Nullable"):
            dtype = f"Nullable({dtype})"
        parts = [f"  `{self.name}` {dtype}"]
        if self.default is not None:
            parts.append(f"DEFAULT {self.default}")
        return " ".join(parts)


@dataclass
class TableDef:
    name: str
    columns: List[ColumnDef]
    primary_key: List[str]

    def order_by(self) -> List[str]:
        if self.primary_key:
            return self.primary_key
        for column in self.columns:
            if not column.nullable:
                return [column.name]
        return ["tuple()"]

    def render(self) -> str:
        columns_sql = ",\n".join(col.render() for col in self.columns)
        formatted_order = []
        for column in self.order_by():
            if column == "tuple()":
                formatted_order.append(column)
            else:
                formatted_order.append(f"`{column}`")
        order = ", ".join(formatted_order)
        return (
            f"DROP TABLE IF EXISTS `{self.name}`;\n"
            f"CREATE TABLE IF NOT EXISTS `{self.name}` (\n"
            f"{columns_sql}\n"
            f") ENGINE = MergeTree\n"
            f"ORDER BY ({order});\n"
        )


def convert_type(mysql_type: str) -> str:
    mysql_type = mysql_type.strip()
    for pattern, replacement in TYPE_MAP:
        if isinstance(replacement, str):
            if pattern.search(mysql_type):
                return replacement
        else:
            match = pattern.search(mysql_type)
            if match:
                return replacement(match)
    return "String"


def parse_column(line: str) -> ColumnDef:
    line = line.rstrip(",")
    match = re.match(r"`([^`]+)`\s+([^\s]+)(.*)", line.strip(), re.I)
    if not match:
        raise ValueError(f"Unable to parse column definition: {line}")
    name, mysql_type, rest = match.groups()
    name = name.lower()
    column_type = convert_type(mysql_type)
    nullable = "NOT NULL" not in rest.upper()
    default_match = re.search(r"DEFAULT\s+([^\s,]+)", rest, re.I)
    default = None
    if default_match:
        default_value = default_match.group(1)
        if default_value.upper() == "CURRENT_TIMESTAMP":
            default = "now()"
        else:
            default = default_value
    return ColumnDef(
        name=name,
        mysql_type=mysql_type,
        clickhouse_type=column_type,
        nullable=nullable,
        default=default,
    )


def parse_table(block: List[str]) -> TableDef:
    header = block[0]
    name_match = re.match(r"CREATE TABLE `([^`]+)`", header, re.I)
    if not name_match:
        raise ValueError(f"Unable to determine table name from line: {header}")
    name = name_match.group(1)
    columns: List[ColumnDef] = []
    primary_key: List[str] = []
    for line in block[1:]:
        stripped = line.strip().rstrip(",")
        upper = stripped.upper()
        if (
            not stripped
            or upper.startswith("KEY ")
            or upper.startswith("UNIQUE KEY")
            or upper.startswith("UNIQUE INDEX")
            or upper.startswith("UNIQUE ")
            or upper.startswith("FULLTEXT KEY")
            or upper.startswith("INDEX ")
        ):
            continue
        if upper.startswith("PRIMARY KEY"):
            cols = re.findall(r"`([^`]+)`", stripped)
            primary_key = [col.lower() for col in cols]
            continue
        if upper.startswith("CONSTRAINT") or upper.startswith("FOREIGN KEY"):
            continue
        if stripped.startswith(")"):
            break
        columns.append(parse_column(stripped))
    return TableDef(name=name, columns=columns, primary_key=primary_key)


def extract_create_blocks(schema_text: str) -> List[List[str]]:
    blocks = []
    lines = schema_text.splitlines()
    current: List[str] = []
    in_block = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        if stripped.upper().startswith("CREATE TABLE"):
            in_block = True
            current = [line]
            continue
        if in_block:
            current.append(line)
            if stripped.endswith(";") and stripped.startswith(")"):
                blocks.append(current)
                current = []
                in_block = False
    return blocks


def convert_schema(mysql_schema_path: pathlib.Path) -> str:
    schema_text = mysql_schema_path.read_text()
    blocks = extract_create_blocks(schema_text)
    tables = [parse_table(block) for block in blocks]
    existing_tables = {table.name for table in tables}
    header = (
        f"-- Autogenerated ClickHouse schema based on {mysql_schema_path.name}\n"
        f"-- Generated on {dt.datetime.utcnow().isoformat()}Z\n"
    )
    rendered_tables = "\n".join(table.render() for table in tables)
    extras = [
        sql for name, sql in ADDITIONAL_CLICKHOUSE_TABLES.items() if name not in existing_tables
    ]
    if extras:
        rendered_tables += "\n" + "\n".join(extras)
    return header + rendered_tables


def convert_seed(mysql_seed_path: pathlib.Path) -> str:
    output_lines = [
        f"-- Autogenerated ClickHouse seed derived from {mysql_seed_path.name}",
        f"-- Generated on {dt.datetime.utcnow().isoformat()}Z",
    ]
    current: List[str] = []
    context = SeedContext()
    for raw_line in mysql_seed_path.read_text().splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        if stripped.upper().startswith("LOCK TABLES") or stripped.upper().startswith("UNLOCK TABLES"):
            continue
        if stripped.startswith("/*!") or stripped.upper().startswith("SET "):
            continue
        current.append(stripped)
        if stripped.endswith(";"):
            statement = " ".join(current)
            current = []
            context = process_seed_statement(statement, output_lines, context)
    return "\n".join(output_lines) + "\n"


def process_seed_statement(
    statement: str,
    output_lines: List[str],
    context: "SeedContext",
) -> "SeedContext":
    upper = statement.upper()
    if upper.startswith("DELETE FROM"):
        table_token = statement.split()[2].strip('";`')
        output_lines.append(f"TRUNCATE TABLE IF EXISTS `{table_token}`;")
        return context
    if upper.startswith("SET @MAX_ENTITY_ID"):
        return context
    if upper.startswith('INSERT INTO "GENETIC_ENTITY"'):
        context.genetic_entity_counter += 1
        context.last_genetic_entity_id = context.genetic_entity_counter
        statement = ensure_genetic_entity_id(statement, context.genetic_entity_counter)
    elif upper.startswith('INSERT INTO "GENE"') and "@max_entity_id" in statement.lower():
        if context.last_genetic_entity_id is None:
            raise ValueError("Encountered @max_entity_id before any genetic_entity insert.")
        statement = statement.replace("@max_entity_id", str(context.last_genetic_entity_id))
    if upper.startswith("INSERT INTO"):
        statement = re.sub(r'"([A-Za-z0-9_]+)"', r'`\1`', statement)
        statement = lowercase_insert_columns(statement)
    normalized = statement.upper()
    if normalized.startswith("INSERT INTO `genetic_entity`"):
        parsed = parse_insert_single_row(statement)
        if parsed:
            _, columns, values = parsed
            record_id = get_numeric_value(columns, values, "ID", context.last_genetic_entity_id)
            stable_idx = get_column_index(columns, "STABLE_ID")
            if stable_idx is not None:
                stable_id = strip_enclosing_quotes(values[stable_idx])
                if stable_id:
                    context.entity_by_stable_id[stable_id] = record_id
    elif normalized.startswith("INSERT INTO `gene`"):
        parsed = parse_insert_single_row(statement)
        if parsed:
            _, columns, values = parsed
            record_id = get_numeric_value(columns, values, "GENETIC_ENTITY_ID")
            entrez_idx = get_column_index(columns, "ENTREZ_GENE_ID")
            if entrez_idx is not None:
                entrez_value = strip_enclosing_quotes(values[entrez_idx])
                try:
                    context.gene_by_entrez[int(entrez_value)] = record_id
                except ValueError as exc:
                    raise ValueError(f"Unable to parse ENTREZ_GENE_ID '{entrez_value}'") from exc
    elif normalized.startswith("INSERT INTO `genetic_alteration`"):
        statement = substitute_selects(statement, context)
    output_lines.append(statement)
    return context


def lowercase_insert_columns(statement: str) -> str:
    upper = statement.upper()
    values_idx = upper.find("VALUES")
    into_idx = upper.find("INTO")
    if values_idx == -1 or into_idx == -1:
        return statement
    first_paren = statement.find("(", into_idx)
    if first_paren == -1 or first_paren > values_idx:
        return statement
    closing = find_matching_paren(statement, first_paren)
    columns_segment = statement[first_paren + 1 : closing]
    columns = [token.strip() for token in columns_segment.split(",") if token.strip()]
    normalized = [f"`{normalize_identifier(column).lower()}`" for column in columns]
    new_columns = "(" + ", ".join(normalized) + ")"
    return statement[:first_paren] + new_columns + statement[closing + 1 :]


def ensure_genetic_entity_id(statement: str, new_id: int) -> str:
    # inject ID column if missing
    columns_start = statement.index("(")
    columns_end = statement.index(")", columns_start)
    columns_segment = statement[columns_start + 1 : columns_end]
    normalized_columns = columns_segment.replace("`", "").replace('"', "").replace(" ", "")
    if "ID" not in normalized_columns.split(","):
        statement = (
            statement[: columns_start + 1]
            + '"ID",'
            + statement[columns_start + 1 :]
        )
        columns_end += len('"ID",')
        values_keyword = statement.upper().index("VALUES", columns_end)
        values_start = statement.index("(", values_keyword)
        statement = (
            statement[: values_start + 1]
            + f"{new_id},"
            + statement[values_start + 1 :]
        )
    return statement


@dataclass
class SeedContext:
    genetic_entity_counter: int = 0
    last_genetic_entity_id: Optional[int] = None
    gene_by_entrez: Dict[int, int] = field(default_factory=dict)
    entity_by_stable_id: Dict[str, int] = field(default_factory=dict)


GENE_SELECT_PATTERN = re.compile(
    r"\(Select\s+[`\"]?GENETIC_ENTITY_ID[`\"]?\s+from\s+[`\"]?gene[`\"]?\s+where\s+[`\"]?ENTREZ_GENE_ID[`\"]?\s*=\s*(\d+)\s*\)",
    re.I,
)

GENERIC_ENTITY_PATTERN = re.compile(
    r"\(Select\s+[`\"]?ID[`\"]?\s+from\s+[`\"]?genetic_entity[`\"]?\s+where\s+[`\"]?STABLE_ID[`\"]?\s*=\s*'([^']+)'\s*\)",
    re.I,
)


def substitute_selects(statement: str, context: SeedContext) -> str:
    def replace_gene(match: re.Match[str]) -> str:
        entrez = int(match.group(1))
        if entrez not in context.gene_by_entrez:
            raise ValueError(f"Missing genetic entity ID for ENTREZ_GENE_ID {entrez}")
        return str(context.gene_by_entrez[entrez])

    def replace_generic(match: re.Match[str]) -> str:
        stable_id = match.group(1)
        if stable_id not in context.entity_by_stable_id:
            raise ValueError(f"Missing genetic entity ID for STABLE_ID '{stable_id}'")
        return str(context.entity_by_stable_id[stable_id])

    statement = GENE_SELECT_PATTERN.sub(replace_gene, statement)
    statement = GENERIC_ENTITY_PATTERN.sub(replace_generic, statement)
    return statement


def parse_insert_single_row(statement: str) -> Optional[tuple[str, List[str], List[str]]]:
    stmt = statement.strip().rstrip(";")
    match = re.match(r"INSERT INTO\s+([`\"]?[\w]+[`\"]?)", stmt, re.I)
    if not match:
        return None
    table_token = normalize_identifier(match.group(1))
    columns_start = stmt.find("(", match.end())
    if columns_start == -1:
        return None
    columns_end = find_matching_paren(stmt, columns_start)
    columns_segment = stmt[columns_start + 1 : columns_end]
    values_keyword = stmt.upper().find("VALUES", columns_end)
    if values_keyword == -1:
        return None
    values_start = stmt.find("(", values_keyword)
    if values_start == -1:
        return None
    values_end = find_matching_paren(stmt, values_start)
    values_segment = stmt[values_start + 1 : values_end]
    columns = [normalize_identifier(token) for token in columns_segment.split(",")]
    values = split_sql_values(values_segment)
    if len(columns) != len(values):
        return None
    return table_token, columns, values


def normalize_identifier(token: str) -> str:
    return token.replace("`", "").replace('"', "").strip()


def split_sql_values(segment: str) -> List[str]:
    items: List[str] = []
    current: List[str] = []
    quote_char: Optional[str] = None
    escape = False
    paren_depth = 0
    for char in segment:
        if escape:
            current.append(char)
            escape = False
            continue
        if quote_char and char == "\\":
            current.append(char)
            escape = True
            continue
        if char in ("'", '"'):
            current.append(char)
            if quote_char == char:
                quote_char = None
            elif quote_char is None:
                quote_char = char
            continue
        if char == "(" and quote_char is None:
            paren_depth += 1
            current.append(char)
            continue
        if char == ")" and quote_char is None:
            if paren_depth > 0:
                paren_depth -= 1
            current.append(char)
            continue
        if char == "," and quote_char is None and paren_depth == 0:
            items.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    if current:
        items.append("".join(current).strip())
    return items


def find_matching_paren(text: str, start_index: int) -> int:
    depth = 0
    for idx in range(start_index, len(text)):
        char = text[idx]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return idx
    raise ValueError("Unbalanced parentheses in INSERT statement.")


def get_column_index(columns: List[str], target: str) -> Optional[int]:
    target_upper = target.upper()
    for idx, column in enumerate(columns):
        if column.upper() == target_upper:
            return idx
    return None


def get_numeric_value(
    columns: List[str],
    values: List[str],
    column_name: str,
    default: Optional[int] = None,
) -> int:
    idx = get_column_index(columns, column_name)
    if idx is None:
        if default is None:
            raise ValueError(f"Missing column {column_name} in INSERT statement.")
        return default
    raw_value = strip_enclosing_quotes(values[idx])
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(f"Unable to parse integer value '{raw_value}' for column {column_name}") from exc


def strip_enclosing_quotes(value: str) -> str:
    trimmed = value.strip()
    if len(trimmed) >= 2 and ((trimmed[0] == "'" and trimmed[-1] == "'") or (trimmed[0] == '"' and trimmed[-1] == '"')):
        inner = trimmed[1:-1]
        return inner.replace("''", "'")
    return trimmed


def main():
    parser = argparse.ArgumentParser(description="Convert MySQL schema/seed into ClickHouse-compatible SQL.")
    parser.add_argument("--mysql-schema", required=True, type=pathlib.Path)
    parser.add_argument("--mysql-seed", required=True, type=pathlib.Path)
    parser.add_argument("--output-schema", required=True, type=pathlib.Path)
    parser.add_argument("--output-seed", required=True, type=pathlib.Path)
    args = parser.parse_args()

    schema_sql = convert_schema(args.mysql_schema)
    seed_sql = convert_seed(args.mysql_seed)

    args.output_schema.parent.mkdir(parents=True, exist_ok=True)
    args.output_schema.write_text(schema_sql)

    args.output_seed.parent.mkdir(parents=True, exist_ok=True)
    args.output_seed.write_text(seed_sql)

    print(f"Wrote ClickHouse schema to {args.output_schema}")
    print(f"Wrote ClickHouse seed to {args.output_seed}")


if __name__ == "__main__":
    main()
