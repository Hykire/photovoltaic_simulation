FROM python:3.9-slim

COPY requirements.txt .

RUN set -eux; \
    \
    pip install --upgrade pip; \
    apt-get update; \
    apt-get upgrade; \
    apt-get -y install libpq-dev gcc; \
    pip install -r requirements.txt; \
    mkdir /opt/pv_simulator;

COPY *.py /opt/pv_simulator/
WORKDIR /opt/pv_simulator/
