FROM apache/airflow:3.3.1-python3.13

# pinning airflow itself in the same install keeps a transitive dependency from
# silently moving the base image's version
USER airflow
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      apache-airflow-providers-postgres \
      --constraint "${HOME}/constraints.txt"

# This image runs the Airflow side only: greek_cc_manifest_dag.py, which reaches
# index.py/db.py/crawls.py and nothing else. Extraction is the `greek-cc-extract`
# CLI, run on demand outside Airflow (see README), so the whole extraction tree
# -- datatrove, spacy/trafilatura, fasttext and its 1.57GB GlotLID model, and the
# build-essential/libmagic1 those needed -- is deliberately absent here. That is
# what greek-cc[dag] selects. Building it in cost ~15 minutes and was the step
# that broke the image when huggingface.co stalled mid-download on this Pi.
# To build an extraction image instead, swap the extra below for [extract] and
# restore the apt layer and the model bake (see git history for both).

USER airflow
# Install dependencies from pyproject.toml alone (read straight out of it, so
# there's no second copy of the list to keep in sync). Sits above COPY src/ so
# editing a module invalidates only the final cheap install.
# Deliberately unconstrained: Airflow's constraints file pins exact versions for
# its own dependency set, which routinely conflicts with fast-moving packages
# like huggingface_hub/boto3/datatrove; Airflow's own docs recommend installing
# extra packages unconstrained in a second step rather than chasing every pin.
COPY --chown=airflow:root pyproject.toml /opt/airflow/greek_cc_src/pyproject.toml
COPY --chown=airflow:root README.md /opt/airflow/greek_cc_src/README.md
RUN python3 -c "\
import subprocess, sys, tomllib; \
deps = tomllib.load(open('/opt/airflow/greek_cc_src/pyproject.toml','rb'))['project']['optional-dependencies']['dag']; \
subprocess.check_call([sys.executable,'-m','pip','install','--no-cache-dir',*deps])"

# index.py pulls each crawl's manifest with hf_hub_download, which writes under
# $HOME/.cache/huggingface at runtime. The container runs as ${AIRFLOW_UID}:0
# (host UID, root group -- see docker-compose.yaml), not this build stage's
# "airflow" user (uid 50000), so that uid cannot create the directory itself
# inside a home it doesn't own. Creating it group-writable here matches the
# convention /opt/airflow already uses for the same reason.
RUN mkdir -p /home/airflow/.cache/huggingface \
    && chmod -R g+rwX /home/airflow/.cache

# Only the project source lands here, and only this last cheap layer rebuilds
# when it changes -- --no-deps because everything it needs is above.
COPY --chown=airflow:root src/ /opt/airflow/greek_cc_src/src/
RUN pip install --no-cache-dir --no-deps /opt/airflow/greek_cc_src
