#!/bin/bash
# Run Script

#Create multiple files in parallel
./genWorkload -o rwriteFsync -n 90 -s 5m -i 4k -p /mnt/fsonloop/ -t 90
./genWorkload -o delete -n 90 -s 5m -i 4k -p /mnt/fsonloop/ -t 10
./genWorkload -o rwriteFsync -n 50 -s 1m -i 4k -p /mnt/fsonloop/ -t 50
