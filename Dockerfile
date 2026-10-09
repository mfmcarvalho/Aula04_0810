FROM quay.io/astronomer/astro-runtime:3.2-9

# --- venv isolado para o dbt (Cosmos vai usar este binário) ---
RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && \
    deactivate
