#!/bin/bash

# LOGGING

# Create log directory
sudo mkdir -p /var/log/sns-crawler/

# DATABASE

# Create nodes index
mongo ./master/mongodb/create_db.js

