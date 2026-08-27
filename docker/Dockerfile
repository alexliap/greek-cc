FROM apache/airflow:3.3.1-python3.13

# pinning airflow itself in the same install keeps a transitive dependency from
# silently moving the base image's version
USER airflow
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      apache-airflow-providers-postgres \
      --constraint "${HOME}/constraints.txt"

# Ordering below is deliberate: the slow steps (compiling fasttext, downloading
# the 1.57GB model) depend only on pyproject.toml, so they sit *above* COPY src/.
# Editing extract.py then invalidates just the final cheap install instead of
# triggering a ~15 minute rebuild.

USER root
# fasttext-numpy2-wheel (GlotLID's backend) has no prebuilt aarch64 wheel and
# builds from source, needing a C++17 compiler the base image doesn't ship.
# libmagic1 is the native C library python-magic (pip package) wraps via
# ctypes -- the pip package alone doesn't provide it, so mime-type detection
# in the extraction pipeline (warc_reader.py -> process_record) fails without it.
RUN apt-get update && apt-get install -y --no-install-recommends build-essential libmagic1 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# Install dependencies from pyproject.toml alone (read straight out of it, so
# there's no second copy of the list to keep in sync).
# Deliberately unconstrained: Airflow's constraints file pins exact versions for
# its own dependency set, which routinely conflicts with fast-moving packages
# like huggingface_hub/boto3/datatrove; Airflow's own docs recommend installing
# extra packages unconstrained in a second step rather than chasing every pin.
COPY --chown=airflow:root pyproject.toml /opt/airflow/greek_cc_src/pyproject.toml
COPY --chown=airflow:root README.md /opt/airflow/greek_cc_src/README.md
RUN python3 -c "\
import subprocess, sys, tomllib; \
deps = tomllib.load(open('/opt/airflow/greek_cc_src/pyproject.toml','rb'))['project']['dependencies']; \
subprocess.check_call([sys.executable,'-m','pip','install','--no-cache-dir',*deps])"

# Bake the GlotLID language-ID model (1.57GB) into the image at build time.
# LanguageFilter(backend="glotlid") lazily downloads it from huggingface.co on
# first use via datatrove's cached_asset_path_or_download -- on a slow
# connection that download has repeatedly died mid-stream (DNS resolution
# failures, read timeouts), killing the whole task. Calling the exact same
# function here, at build time, writes the file to the same cache path +
# ".completed" marker runtime looks for, so extraction never touches the
# network for it.
RUN python3 -c "from datatrove.io import cached_asset_path_or_download; cached_asset_path_or_download('hf://cis-lmu/glotlid/model_v3.bin', namespace='lid', subfolder='glotlid', desc='GlotLID model')"

# The container actually runs as ${AIRFLOW_UID}:0 (host UID, root group -- see
# docker-compose.yaml), not this build stage's "airflow" user (uid 50000), so
# the cache tree above got created owner-writable-only (755) under a uid that
# doesn't match the runtime one. Other datatrove steps (e.g. URLFilter) lazily
# create sibling directories under the same ~/.cache/huggingface/assets/datatrove/
# at runtime -- without this, that mkdir hits PermissionError since the runtime
# UID isn't the owner and the group only had read+execute. Matches the
# group-writable convention /opt/airflow itself already uses for the same reason.
RUN chmod -R g+rwX /home/airflow/.cache

# Only the project source lands here, and only this last cheap layer rebuilds
# when it changes -- --no-deps because everything it needs is above.
COPY --chown=airflow:root src/ /opt/airflow/greek_cc_src/src/
RUN pip install --no-cache-dir --no-deps /opt/airflow/greek_cc_src
