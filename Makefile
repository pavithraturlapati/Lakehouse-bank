PY ?= python

.PHONY: setup generate changes bronze silver quality demo test clean

setup:      ## install dependencies
	$(PY) -m pip install -r requirements.txt

generate:   ## first batch: initial load of customers
	$(PY) -m lakehouse.generate_data

changes:    ## next batch: updates, new customers, deletes
	$(PY) -m lakehouse.generate_data --changes 50

bronze:
	$(PY) -m lakehouse.bronze

silver:
	$(PY) -m lakehouse.silver

quality:
	$(PY) -m lakehouse.quality

demo: clean  ## full end-to-end run
	$(MAKE) generate bronze silver
	$(MAKE) changes bronze silver quality

test:
	$(PY) -m pytest -q

clean:      ## delete all generated data and tables
	rm -rf data spark-warehouse metastore_db derby.log
