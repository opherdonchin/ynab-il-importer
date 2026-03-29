"""YNAB IL importer package.

The repository is organized around a staged workflow:

- input normalization and identity extraction
- pairing / matching / rule application
- human review in ``review_app``
- upload preparation and reconciliation

If you are reading the codebase for the first time, start with:

1. ``documents/architecture_overview.md`` for the stage-to-module map
2. ``documents/project_context.md`` for the product goals
3. ``src/ynab_il_importer/cli.py`` and ``scripts/`` for runnable entry points
4. ``src/ynab_il_importer/review_app/`` for the current review workflow core
"""

__all__ = ["__version__"]
__version__ = "0.1.0"
