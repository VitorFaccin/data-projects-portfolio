FROM apache/airflow:2.9.2-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org \
    --trusted-host pypi.python.org \
    -r /tmp/airflow-requirements.txt

# Install the project package in editable mode so imports work without PYTHONPATH tricks.
# src/ is also mounted as a volume at runtime — the editable install keeps pointing to that path,
# so code changes on the host are reflected immediately without rebuilding the image.
COPY --chown=airflow:root pyproject.toml /opt/airflow/pyproject.toml
COPY --chown=airflow:root src/ /opt/airflow/src/
RUN pip install --no-cache-dir -e /opt/airflow/
