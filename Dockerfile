FROM apache/airflow:2.10.5-python3.11

COPY requirements.txt /tmp/requirements.txt

# Constraints file keeps our extra packages compatible with Airflow's own pinned dependencies.
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
