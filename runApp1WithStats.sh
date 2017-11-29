#!/bin/bash

source /home/ubuntu/scripts/getstats.sh
echo "Start" $(date +%s)
./runApp1.sh
echo "End: " $(date +%s)
source /home/ubuntu/scripts/getstats.sh
