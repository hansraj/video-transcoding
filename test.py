"""
A simple test script to run with randomly selected seeks.
Note : we know the length of the input file for which we are doing seek.
"""

import subprocess
import sys 
import time
import random
import os 
from datetime import datetime as dt

# Presets : 0->xvid, 1->WebM(default), 2->Theora
presets = [["-p", "xvid", "-d", "xvid"], [], ["-p", "Theora"]]
extensions = ['mp4', 'webm', 'mkv']

DEFAULT_RUNS = 10
num_runs = int(sys.argv[1]) if len(sys.argv) == 2 else DEFAULT_RUNS 

datestamp = dt.now().strftime("%Y%m%d")
datestamp_dir = "/tmp/%s" % datestamp
if not os.path.exists(datestamp_dir):
    os.mkdir(datestamp_dir)

run = 0
while run < num_runs:
    offset = random.randint(0,len(presets)-1)
    start = random.randint(1,50)
    stop = random.randint(1,50)
    if start > stop:
        stop = -1
    start = str(start)
    stop = str(stop)
    input = "/home/xvid/sintel_trailer-480p.ogv"
    output, ext = os.path.splitext(input) 
    outfile = output.split("/")[-1]
    outfile = "%s-%s-%s.%s" % (outfile, start, stop, extensions[offset])
    output = os.path.join("/tmp", datestamp, outfile)
    print input, output, start, stop
    subprocess_array = ["./arista-transcode"] + presets[offset] + \
                        [input, "-o", output, "--start-time", start, \
                         "--stop-time", stop]
    print (" ").join(subprocess_array)
    x = subprocess.call(subprocess_array)
    time.sleep(2)
    run += 1
