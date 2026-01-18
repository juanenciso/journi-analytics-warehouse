.PHONY: help venv install gen bronze silver all test clean

help:
	@echo "Targets:"
	@echo "  make venv     - create venv"
	@echo "  make install  - install deps"
	@echo "  make gen      - generate raw parquet in ./data"
	@echo "  make bronze   - write bronze to ./warehouse/bronze"
	@echo "  make silver   - write silver + dq_report to ./warehouse/silver"
	@echo "  make all      - run gen -> bronze -> silver"
	@echo "  make test     - run tests"
	@echo "  make clean    - remove data/ warehouse/"

venv:
	python3 -m venv .venv

install:
	. .venv/bin/activate && pip install -U pip && pip install -r requirements.txt

gen:
	. .venv/bin/activate && python -m src.generate_data

bronze:
	. .venv/bin/activate && python -m src.etl.bronze

silver:
	. .venv/bin/activate && python -m src.etl.silver

all: gen bronze silver

test:
	. .venv/bin/activate && pytest -q

clean:
	rm -rf data warehouse spark-warehouse metastore_db derby.log
