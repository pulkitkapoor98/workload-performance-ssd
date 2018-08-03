#!/bin/bash
# Run Script

#Create multiple files in parallel
./genWorkload -o create -n 100 -s 50m -i 64k -p /mnt/fsonloop/ -t 10