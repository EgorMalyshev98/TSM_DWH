FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    git gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dbt_project.yml ./
COPY tsm_dwh ./tsm_dwh
COPY profiles.yml ./