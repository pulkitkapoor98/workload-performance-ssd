#!/bin/bash
# Run Script
# Request Scale Workload

# In favour of rule 
./genWorkload -o swrite -n 100 -s 5m -i 4k -p /mnt/fsonloop/ -t 1

# Not in favour of rule
#./genWorkload -o swriteFsync -n 100 -s 5m -i 4k -p /mnt/fsonloop/ -t 1