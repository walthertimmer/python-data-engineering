FROM --platform=$TARGETPLATFORM python:3.13-alpine

LABEL org.opencontainers.image.description="Python 3.13 with Polars (Alpine-based)"

# Install required build dependencies
RUN apk add --no-cache \
    gcc \
    musl-dev \
    python3-dev \
    rust \
    cargo \
    make

# Install Python dependencies
COPY Docker/PolarsImage_requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /scripts

# Container will expect volume mount at /scripts
VOLUME /scripts

### Sleep infinitely if debugging locally
# ENTRYPOINT ["tail", "-f", "/dev/null"]