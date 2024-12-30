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
# ENV HADOOP_OPTIONAL_TOOLS=""
# ENV HADOOP_JAAS_DEBUG=true
# ENV SPARK_HADOOP_SECURITY_AUTH=simple
# ENV SPARK_SECURITY_CREDENTIALS=false
# ENV HADOOP_SECURITY_AUTHENTICATION=simple
# ENV HADOOP_SECURITY_AUTHORIZATION=false
# ENV HADOOP_OPTS="-Djava.security.krb5.conf=/dev/null -Djavax.security.auth.useSubjectCredsOnly=false -Djavax.security.auth.login.config=/dev/null"
# ENV SPARK_JAVA_OPTS="-Djava.security.krb5.conf=/dev/null -Djavax.security.auth.useSubjectCredsOnly=false -Djavax.security.auth.login.config=/dev/null"
# ENV JAVA_SECURITY_KRB5_CONF=/dev/null
# ENV HADOOP_SECURITY_AUTH_TO_LOCAL="RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*// DEFAULT"
# ENV SPARK_AUTH_TO_LOCAL=RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//
# ENV SPARK_HADOOP_USER_NAME=root
# ENV HADOOP_USER_NAME=root
# ENV SPARK_USER=root
# ENV HADOOP_PROXY_USER=root
# ENV SPARK_PROXY_USER=root
# ENV KRB5CCNAME=/dev/null
# ENV HADOOP_HOME=/tmp/hadoop
# ENV HADOOP_CONF_DIR=/tmp/hadoop/conf

# RUN mkdir -p /tmp/spark && \
#     chmod -R 777 /tmp/spark && \
#     chown -R root:root /tmp/spark

# Update the core-site.xml configuration
# RUN mkdir -p /tmp/hadoop/conf && \
#     echo '<configuration> \
#     <property> \
#         <name>hadoop.security.authentication</name> \
#         <value>simple</value> \
#     </property> \
#     <property> \
#         <name>hadoop.proxyuser.root.hosts</name> \
#         <value>*</value> \
#     </property> \
#     <property> \
#         <name>hadoop.proxyuser.root.groups</name> \
#         <value>*</value> \
#     </property> \
#     </configuration>' > /tmp/hadoop/conf/core-site.xml && \
#     chmod -R 777 /tmp/hadoop

# Update the PAM configuration / Create empty krb5.conf
# RUN rm -f /etc/krb5.conf && \
#     touch /etc/krb5.conf && \
#     chmod 644 /etc/krb5.conf

# Install Python dependencies
COPY Docker/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Copy ETL scripts
COPY PythonScripts/ /scripts/

WORKDIR /scripts

### Sleep infinitely
# ENTRYPOINT ["tail", "-f", "/dev/null"]