# pyspark_test_task

## Steps:

- Check Python version (it must be 3.7.* or less for PySpark 2.4.3) or install it via pyenv;
- Create virtual environment via ```python3 -m venv env```;
- Activate virtual environment via ```source env/bin/activate```;
- Install requirements via ```pip3 install -r requirements.txt```;
- Paste your path for input parquet file in param ```input_path``` in ```task.py```;
- Paste your path for output parquet file in param ```output_path``` in ```task.py```;

# Paths must be different

## Execute task.py and check output path