# Data Loader App (DLA)

## Overview
This is a simple tool to map, transform and load data from various sources (flat files, e.g.) into some target database (for now, just the goal is to just support relational DBs).

## Getting started

### Requirements:
* Python 3.10

### How to set up
Create a new Python virtual environment with the command:
```bash
python3.10 -m venv venv
```

Activate virtual environment:
```bash
source venv/bin/activate
```

Install requirements:
```bash
pip install -r requirements.txt
```

Install application:
```bash
pip install -e .
```

Run tests:
```bash
PYTHONPATH=$(pwd) pytest -m unit # unit tests
PYTHONPATH=$(pwd) pytest -m integration # integration tests
```


### CLI Usage
Example of current CLI usage (after running `pip install` command above)
```bash
data_loader --dataset_name sales --data_path data/examples/sales.csv
```
