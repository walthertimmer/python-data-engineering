FROM python:3.13

LABEL org.opencontainers.image.description="Python 3.13 with OpenJDK 17 for PySpark applications"

# Install OpenJDK
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Directory for ivy cache
RUN mkdir -p /tmp/.ivy2 && \
    chmod 777 /tmp/.ivy2

# Install Python dependencies
COPY Docker/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Copy ETL scripts
# COPY PythonScripts/ /scripts/

WORKDIR /scripts

# Add an entrypoint script that pulls latest code before running
COPY Docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

### Sleep infinitely if debugging locally
# ENTRYPOINT ["tail", "-f", "/dev/null"]

# run the entrypoint script (get latest python scripts)
ENTRYPOINT ["/entrypoint.sh"]