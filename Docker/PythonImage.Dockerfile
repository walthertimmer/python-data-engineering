FROM python:3.13

COPY Docker/requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt

COPY PythonScripts/ /scripts/