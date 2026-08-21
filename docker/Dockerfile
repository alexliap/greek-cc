FROM apache/airflow:3.3.1-python3.13

USER root
COPY pyproject.toml /opt/airflow/greek_cc_src/pyproject.toml
COPY README.md /opt/airflow/greek_cc_src/README.md
COPY src/ /opt/airflow/greek_cc_src/src/
RUN chown -R airflow:root /opt/airflow/greek_cc_src

USER airflow
# pinning airflow itself in the same install keeps a transitive dependency from
# silently moving the base image's version
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      apache-airflow-providers-postgres \
      /opt/airflow/greek_cc_src \
      --constraint "${HOME}/constraints.txt"
