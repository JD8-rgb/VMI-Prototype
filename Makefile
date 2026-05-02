# Convenience targets for the VMI prototype.
# Each target stands on its own — no inter-target deps so you can run
# any one of them in isolation. PYTHON defaults to python3; override
# at the command line: `make PYTHON=py3 test`.

PYTHON ?= python3

.PHONY: help test test-quick test-parser test-pytest mypy mypy-strict \
        smoke lint-customers clean install install-dev

help:
	@echo "Targets:"
	@echo "  test            Run parser stress + pytest (~1.5s, the standard pre-commit gate)"
	@echo "  test-quick      pytest only (~1s)"
	@echo "  test-parser     Schedule parser stress harness only"
	@echo "  test-pytest     Pytest only (alias of test-quick)"
	@echo "  mypy            Default-mode mypy on the algorithm core"
	@echo "  mypy-strict     --strict on the leaf modules (data_io, state, config)"
	@echo "  lint-customers  Run validate_customer.py over customers/"
	@echo "  smoke           Quick CLI smoke (tank_status, plan_orders --customer)"
	@echo "  install         pip install -r requirements.txt"
	@echo "  install-dev     pip install -r requirements-dev.txt (adds pytest)"
	@echo "  clean           Remove __pycache__/ and .pytest_cache/"

test: test-parser test-pytest

test-quick: test-pytest

test-parser:
	PYTHONIOENCODING=utf-8 $(PYTHON) test_schedule_parser.py --regex-only

test-pytest:
	PYTHONIOENCODING=utf-8 $(PYTHON) -m pytest tests/ -q

mypy:
	$(PYTHON) -m mypy --ignore-missing-imports \
	    alerts.py plan_orders.py projection.py state.py \
	    config.py data_io.py time_utils.py

mypy-strict:
	$(PYTHON) -m mypy --strict --ignore-missing-imports \
	    state.py config.py data_io.py

lint-customers:
	$(PYTHON) validate_customer.py

smoke:
	@echo "── tank_status (defaults) ──"
	@$(PYTHON) tank_status.py 2>/dev/null | head -5 || true
	@echo ""
	@echo "── tank_status --customer example_customer ──"
	@$(PYTHON) tank_status.py --customer example_customer 2>/dev/null | head -5 || true
	@echo ""
	@echo "── plan_orders --customer example_customer (read-only) ──"
	@$(PYTHON) plan_orders.py --customer example_customer 2>/dev/null | tail -10

install:
	$(PYTHON) -m pip install -r requirements.txt

install-dev:
	$(PYTHON) -m pip install -r requirements-dev.txt

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache
