FROM python:3.11-slim

# Install system dependencies required by PyHive / thrift transport
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libsasl2-dev \
    libssl-dev \
    libffi-dev \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "dbt-core==1.8.0" \
    "dbt-spark[PyHive]==1.8.0"

WORKDIR /dbt

COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["dbt", "run", "--profiles-dir", "/dbt", "--project-dir", "/dbt"]
