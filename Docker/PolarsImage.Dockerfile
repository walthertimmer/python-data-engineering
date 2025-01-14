FROM python:3.13-slim

LABEL org.opencontainers.image.description="Python 3.13 with Polars"

# Install Python dependencies
COPY Docker/PolarsImage_requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /scripts

# Container will expect volume mount at /scripts
VOLUME /scripts

### Sleep infinitely if debugging locally
# ENTRYPOINT ["tail", "-f", "/dev/null"]