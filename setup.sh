#!/bin/bash

# Application

# mkdir -p ~/research
# cd ~/research
# git clone https://github.com/josecarlosgt/sns-crawler.git

# Logging

# Create log directory
sudo mkdir -p /var/log/sns-crawler/
me=`whoami`
sudo chown $me /var/log/sns-crawler/
