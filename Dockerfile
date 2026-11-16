# PySpark runtime for the project: Python 3.12 + Java runtime + requirements.
# Running in Linux avoids Windows-specific Spark issues (winutils / HADOOP_HOME,
# and pyspark 4.1.0 failing to import on Windows).
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The project itself is bind-mounted at /app by docker-compose.yml
CMD ["python", "--version"]
