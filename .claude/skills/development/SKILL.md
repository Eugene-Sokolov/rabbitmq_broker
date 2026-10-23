---
name: development
description: Guidelines on how to write code
---

When adding a dependency (docker image or pip package or else) use latest available version, and pin it's minimum version. For example in `pyproject.toml` would be `beautifulsoup4>=4.15.0`

When writing code don't write comments. It should be self-explanatory

When writing python code:
1. Always use type hints for function arguments and returns 
2. Hide utility functions with underscores, if it's not going to be imported by other packages - they don't need to see it

App has virtual env at root `.venv`. It includes ruff. When finishing writing code, don't forget to format, lint, and fix any linter errors. Don't update linter settings yourself 

On every code change that doesn't have tests - consider adding unit tests

Run unit tests after you've finished writing business logic. For integration and E2E tests ask developer if are relevant

Each DTO should be pydantic class if this package is available
