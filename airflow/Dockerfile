FROM apache/airflow:2.10.5-python3.12

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"

# dbt lives in its own virtualenv: its dependencies would conflict with Airflow's.
USER root
RUN mkdir -p /opt/dbt-venv /opt/airflow/warehouse && chown airflow:root /opt/dbt-venv /opt/airflow/warehouse
USER airflow
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.9.1 dbt-duckdb==1.9.1 duckdb==1.1.3 \
 # pre-download DuckDB's S3 extension so runs don't need to fetch it
 && /opt/dbt-venv/bin/python -c "import duckdb; duckdb.connect().sql('INSTALL httpfs')"
ENV DBT_BIN=/opt/dbt-venv/bin/dbt
