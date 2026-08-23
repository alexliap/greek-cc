FROM apache/airflow:3.3.1-python3.13

USER root
COPY pyproject.toml /opt/airflow/greek_cc_src/pyproject.toml
COPY README.md /opt/airflow/greek_cc_src/README.md
COPY src/ /opt/airflow/greek_cc_src/src/
RUN chown -R airflow:root /opt/airflow/greek_cc_src

# fasttext-numpy2-wheel (GlotLID's backend) has no prebuilt aarch64 wheel and
# builds from source, needing a C++17 compiler the base image doesn't ship
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# pinning airflow itself in the same install keeps a transitive dependency from
# silently moving the base image's version
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      apache-airflow-providers-postgres \
      --constraint "${HOME}/constraints.txt"

# installed separately, without --constraint: Airflow's constraints file pins
# exact versions for its own dependency set, which routinely conflicts with
# fast-moving packages like huggingface_hub/boto3/datatrove -- Airflow's own
# docs recommend installing extra packages unconstrained in a second step
# rather than chasing every pin by hand
RUN pip install --no-cache-dir /opt/airflow/greek_cc_src
