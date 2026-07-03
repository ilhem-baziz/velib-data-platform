FROM apache/spark:3.5.6

USER root

RUN apt-get update \
    && apt-get install -y python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=/opt/app/src

USER spark