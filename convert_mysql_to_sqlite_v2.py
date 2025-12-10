#!/usr/bin/env python3
"""
Convert MySQL dump to SQLite-compatible SQL format.
Properly handles:
- Multi-line CREATE TABLE statements
- KEY definitions embedded within column definitions
- Complex escaping in INSERT statements
"""

import sys
import re
from pathlib import Path

def convert_mysql_to_sqlite(input_file, output_file):
    """Convert MySQL dump to SQLite format with proper multi-line handling."""

    print(f"Converting {input_file} to SQLite format...")
    print(f"Output: {output_file}")

    input_path = Path(input_file)
    file_size = input_path.stat().st_size
    bytes_processed = 0
    lines_processed = 0

    with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:

        buffer = []
        in_statement = False

        for line in infile:
            bytes_processed += len(line.encode('utf-8'))
            lines_processed += 1

            # Progress reporting every 1M lines
            if lines_processed % 1000000 == 0:
                progress = (bytes_processed / file_size) * 100
                print(f"Processed {lines_processed:,} lines ({progress:.1f}%)")

            # Skip MySQL-specific commands
            line_upper = line.strip().upper()
            if any(line_upper.startswith(cmd) for cmd in [
                'SET ', 'USE ', 'LOCK TABLES', 'UNLOCK TABLES',
                '/*!', 'START TRANSACTION', 'COMMIT', '--'
            ]):
                continue

            # Empty line
            if not line.strip():
                continue

            # Start of a statement
            if not in_statement:
                buffer = [line]
                in_statement = True
            else:
                buffer.append(line)

            # Check if statement is complete (ends with semicolon)
            if line.rstrip().endswith(';'):
                in_statement = False
                statement = ''.join(buffer)
                converted = convert_statement(statement)
                if converted:
                    outfile.write(converted)
                buffer = []

    print(f"\nConversion complete!")
    print(f"Total lines processed: {lines_processed:,}")
    print(f"Output file: {output_file}")


def convert_statement(statement):
    """Convert a single SQL statement from MySQL to SQLite."""

    # Convert backticks to double quotes (SQLite's identifier quote character)
    # This preserves identifiers that are SQL reserved keywords like VALUES, KEY, etc.
    statement = statement.replace('`', '"')

    # Convert data types
    statement = re.sub(r'\bDATETIME\b', 'TEXT', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\bTIMESTAMP\b', 'TEXT', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\bTINYINT\(\d+\)', 'INTEGER', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\bSMALLINT\(\d+\)', 'INTEGER', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\bMEDIUMINT\(\d+\)', 'INTEGER', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\bINT\(\d+\)', 'INTEGER', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\bBIGINT\(\d+\)', 'INTEGER', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\bDOUBLE\b', 'REAL', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\bFLOAT\b', 'REAL', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\bLONGTEXT\b', 'TEXT', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\bMEDIUMTEXT\b', 'TEXT', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\bTINYTEXT\b', 'TEXT', statement, flags=re.IGNORECASE)

    # Convert ENUM to TEXT (SQLite doesn't support ENUM)
    # Example: enum('ASC','DESC') -> TEXT
    statement = re.sub(r'\benum\s*\([^)]+\)', 'TEXT', statement, flags=re.IGNORECASE)

    # Remove AUTO_INCREMENT
    statement = re.sub(r'\bAUTO_INCREMENT\b', '', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\bAUTO_INCREMENT\s*=\s*\d+', '', statement, flags=re.IGNORECASE)

    # Remove ENGINE, CHARSET, COLLATE at end of CREATE TABLE
    statement = re.sub(r'\)\s*ENGINE\s*=\s*\w+[^;]*;', ');', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\s*DEFAULT\s+CHARSET\s*=\s*[\w_]+', '', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\s*CHARSET\s*=\s*[\w_]+', '', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\s*COLLATE\s*=\s*[\w_]+', '', statement, flags=re.IGNORECASE)

    # Remove inline COLLATE clauses
    statement = re.sub(r'\s+COLLATE\s+[\w_]+', '', statement, flags=re.IGNORECASE)

    # Remove COMMENT clauses
    statement = re.sub(r"\s+COMMENT\s+'[^']*'", '', statement, flags=re.IGNORECASE)
    statement = re.sub(r'\s+COMMENT\s+"[^"]*"', '', statement, flags=re.IGNORECASE)

    # Handle CREATE TABLE statements specially
    if re.match(r'\s*CREATE\s+TABLE', statement, re.IGNORECASE):
        statement = convert_create_table(statement)

    # Handle INSERT statements specially (for escaping)
    if re.match(r'\s*INSERT', statement, re.IGNORECASE):
        statement = convert_insert(statement)

    # Convert REPLACE INTO to INSERT OR REPLACE INTO
    statement = re.sub(r'\bREPLACE\s+INTO\b', 'INSERT OR REPLACE INTO', statement, flags=re.IGNORECASE)

    return statement


def convert_create_table(statement):
    """Convert CREATE TABLE statement, converting UNIQUE KEY to UNIQUE and removing regular KEY definitions."""

    # First pass: Process standalone KEY definitions on their own lines
    lines = statement.split('\n')
    result_lines = []

    for line in lines:
        line_stripped = line.strip()
        line_upper = line_stripped.upper()

        # Convert standalone UNIQUE KEY to UNIQUE
        # Example: "  UNIQUE KEY "UQ_NAME" ("COL1","COL2")," -> "  UNIQUE ("COL1","COL2"),"
        if re.match(r'UNIQUE\s+KEY\s+', line_stripped, re.IGNORECASE):
            # Extract the column list (part in parentheses), handling quoted identifiers
            match = re.search(r'UNIQUE\s+KEY\s+"?[^"\s]+"?\s*(\([^)]+\))', line_stripped, re.IGNORECASE)
            if match:
                # Preserve indentation and trailing comma
                indent = line[:len(line) - len(line.lstrip())]
                trailing = ',' if line_stripped.rstrip().endswith(',') else ''
                converted_line = f"{indent}UNIQUE {match.group(1)}{trailing}"
                result_lines.append(converted_line)
                continue

        # Skip standalone regular KEY/INDEX definitions (not UNIQUE)
        # Important: Distinguish between column definitions and index definitions
        # Column: "KEY" varchar(255) NOT NULL," (quoted KEY identifier, followed by datatype)
        # Index:  KEY "idx_name" ("column1", "column2") (unquoted KEY keyword, quoted name)
        if re.match(r'"?KEY"?\s+', line_stripped, re.IGNORECASE):
            # Check if this starts with quoted "KEY" - that's a column definition
            is_column_def = line_stripped.startswith('"KEY"')
            if is_column_def:
                # Keep column definitions
                pass
            else:
                # Unquoted KEY means it's an index definition, skip it
                continue

        if re.match(r'(FULLTEXT|SPATIAL)\s+(INDEX|KEY)\s+', line_stripped, re.IGNORECASE):
            continue
        if line_upper.startswith('INDEX '):
            continue

        result_lines.append(line)

    statement = '\n'.join(result_lines)

    # Second pass: Handle embedded KEY definitions in column lines

    # Convert embedded UNIQUE KEY to UNIQUE
    # Example: "PRIMARY KEY (ID), UNIQUE KEY `UQ_NAME` (`COL`)" -> "PRIMARY KEY (ID), UNIQUE (`COL`)"
    statement = re.sub(
        r',\s*UNIQUE\s+KEY\s+\S+\s*(\([^)]+\))',
        r', UNIQUE \1',
        statement,
        flags=re.IGNORECASE
    )

    # Remove embedded regular KEY definitions (not UNIQUE)
    # Only match KEY definitions that come after a closing paren (part of compound constraint)
    # This avoids matching column definitions like "KEY varchar(255)"
    # Example: "PRIMARY KEY (ID), KEY idx (column)" -> "PRIMARY KEY (ID)"
    statement = re.sub(
        r'\)\s*,\s*KEY\s+[a-zA-Z_]\S*\s*\([^)]+\)',
        ')',
        statement,
        flags=re.IGNORECASE
    )

    # Clean up multiple consecutive commas
    statement = re.sub(r',\s*,+', ',', statement)

    # Clean up trailing comma before closing parenthesis
    # Example: "  ,\n)" should become "  \n)"
    statement = re.sub(r',(\s*\n\s*\))', r'\1', statement)
    statement = re.sub(r',(\s*\))', r'\1', statement)

    return statement


def convert_insert(statement):
    """Convert INSERT statement, handling MySQL to SQLite escaping."""

    # MySQL uses backslash escaping: \'
    # SQLite prefers doubled single quotes: ''
    #
    # We need to convert \' to '' within string literals
    # This is tricky because we need to:
    # 1. Only process inside single-quoted strings
    # 2. Not break already-correct escaping
    # 3. Handle \\' (escaped backslash before quote)

    # Strategy: Convert \' to '' inside string literals
    # Use a simple state machine to track if we're inside a string

    result = []
    i = 0
    in_string = False

    while i < len(statement):
        char = statement[i]

        if char == "'" and (i == 0 or statement[i-1] != '\\'):
            # Toggle string state (unescaped single quote)
            in_string = not in_string
            result.append(char)
            i += 1
        elif in_string and char == '\\' and i + 1 < len(statement):
            next_char = statement[i+1]
            if next_char == "'":
                # Convert \' to '' inside strings
                result.append("''")
                i += 2  # Skip both \ and '
            elif next_char == '\\':
                # Keep \\ as-is (escaped backslash)
                result.append('\\\\')
                i += 2
            else:
                # Other escape sequences (keep as-is for now)
                # \n, \r, \t, etc. are also supported by SQLite
                result.append(char)
                i += 1
        else:
            result.append(char)
            i += 1

    return ''.join(result)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python3 convert_mysql_to_sqlite_v2.py <input.sql> <output_sqlite.sql>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    if not Path(input_file).exists():
        print(f"Error: Input file '{input_file}' not found")
        sys.exit(1)

    convert_mysql_to_sqlite(input_file, output_file)
