install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

.PHONY: lint
PY_FILES := $(wildcard energy-pipeline/src/*.py)
lint:
	@PYTHONPATH=$(CURDIR)/energy-pipeline/src python3 -m pylint --disable=R,C $(PY_FILES)
	
format:
	black energy-pipeline/src/*.py

all: install lint format


