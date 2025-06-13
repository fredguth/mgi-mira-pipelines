# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands
- Run pipeline: `sqlmesh run`
- Run specific model: `sqlmesh run [model_name]`
- Test pipeline: `sqlmesh test`
- Test specific model: `sqlmesh test [model_name]`
- Audit check: `sqlmesh run audits/[audit_name].sql`

## Code Style Guidelines
- Python: Use type hints, follow PEP 8
- SQL: Lowercase SQL keywords, CamelCase for tables, snake_case for columns
- Error handling: Use try/except with specific exceptions
- Imports: Group standard lib, third-party, and local imports
- Documentation: Add docstrings to all functions and model SQL files
- Models: Use SQLMesh model annotation for Python models or MODEL directive for SQL
- DuckDB syntax: Use DuckDB-specific SQL dialect for optimized queries