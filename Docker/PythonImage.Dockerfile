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

# Set Spark environment variables to disable kerberos security
ENV SPARK_HADOOP_SECURITY_AUTH=simple
ENV SPARK_SECURITY_CREDENTIALS=false
ENV HADOOP_SECURITY_AUTHENTICATION=simple
ENV HADOOP_SECURITY_AUTHORIZATION=false
ENV HADOOP_OPTS="-Djava.security.krb5.conf=/dev/null -Djavax.security.auth.useSubjectCredsOnly=false"
ENV SPARK_JAVA_OPTS="-Djava.security.krb5.conf=/dev/null -Djavax.security.auth.useSubjectCredsOnly=false"
ENV JAVA_SECURITY_KRB5_CONF=/dev/null
ENV HADOOP_SECURITY_AUTH_TO_LOCAL=RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//
ENV SPARK_AUTH_TO_LOCAL=RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//
ENV SPARK_HADOOP_USER_NAME=none
ENV SPARK_USER=none
ENV KRB5CCNAME=/dev/null
ENV HADOOP_HOME=/tmp/hadoop
ENV HADOOP_CONF_DIR=/tmp/hadoop/conf

# Update the core-site.xml configuration
RUN mkdir -p /tmp/hadoop/conf && \
    echo '<configuration> \
        <property> \
            <name>hadoop.security.authentication</name> \
            <value>simple</value> \
        </property> \
        <property> \
            <name>hadoop.security.authorization</name> \
            <value>false</value> \
        </property> \
        <property> \
            <name>hadoop.security.credential.provider.path</name> \
            <value></value> \
        </property> \
    </configuration>' > /tmp/hadoop/conf/core-site.xml && \
    chmod -R 777 /tmp/hadoop

RUN echo "auth required pam_permit.so" > /etc/pam.d/common-auth && \
    echo "account required pam_permit.so" > /etc/pam.d/common-account

# Update the PAM configuration / Create empty krb5.conf
RUN rm -f /etc/krb5.conf && \
    touch /etc/krb5.conf && \
    chmod 644 /etc/krb5.conf

# Install Python dependencies
COPY Docker/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Copy ETL scripts
COPY PythonScripts/ /scripts/