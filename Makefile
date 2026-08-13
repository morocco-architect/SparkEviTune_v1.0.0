.PHONY: install dev test api ui demo-data train lint clean

install:
	python -m pip install -e .

dev:
	python -m pip install -e ".[ui,llm,dev]"

test:
	pytest

api:
	uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

ui:
	streamlit run dashboard/app.py

demo-data:
	python scripts/generate_demo_history.py --rows 250

train:
	python scripts/train_models.py

lint:
	ruff check src api dashboard scripts tests

clean:
	rm -rf .pytest_cache .coverage htmlcov build dist *.egg-info
