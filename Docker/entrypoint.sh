#!/bin/bash

# Clone/pull latest code
git clone https://github.com/walthertimmer/python-data-engineering.git /tmp/repo || \
    (cd /tmp/repo && git pull)

# Copy scripts to working directory
cp -r /tmp/repo/PythonScripts/* /scripts/

# Execute the passed command
exec "$@"