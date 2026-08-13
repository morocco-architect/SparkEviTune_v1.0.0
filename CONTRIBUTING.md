# Contributing

1. Create a feature branch.
2. Add or update tests for every behavioral change.
3. Run `ruff check` and `pytest`.
4. Keep deterministic rule behavior auditable.
5. Do not introduce automatic configuration deployment in the analysis service.
6. Document model training data, target construction and evaluation methodology.
7. Never commit API keys, Spark credentials, production event logs or trained models containing sensitive data.
