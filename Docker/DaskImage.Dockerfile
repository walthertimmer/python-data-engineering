FROM python:3.13-slim

LABEL org.opencontainers.image.description="Minimal Python image for Dask ETL operations"

# Install only required system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install only the Python packages needed for the Dask script
RUN pip install --no-cache-dir \
    dask[complete]==2024.12.1 \
    deltalake==0.23.2 \
    python-dotenv==1.0.1 \
    boto3==1.35.85 \
    s3fs==2024.12.0 \
    pyarrow==18.1.0 \
    psutil==6.1.1

# Copy ETL scripts
COPY PythonScripts/ /scripts/

# Create working directory
WORKDIR /scripts