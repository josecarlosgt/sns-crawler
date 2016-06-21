#!/bin/bash

# Logging

# Create log directory
sudo mkdir -p /var/log/sns-crawler/

# Application

mkdir -p ~/research
cd ~/research
git clone https://github.com/josecarlosgt/sns-crawler.git
