# Data Loader App (DLA)

## Getting started
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

Run tests:
```bash
PYTHONPATH=$(pwd) pytest -m unit # unit tests
PYTHONPATH=$(pwd) pytest -m integration # integration tests
```
