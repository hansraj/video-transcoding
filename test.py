#!/usr/bin/env python
"""
A simple test script to run with randomly selected options. Only relative
options are supported right now (good enough for phase 1).
Takes number of runs as command line option. Default is 10
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
num_runs = int(sys.argv[1]) if len(sys.argv) >= 2 else DEFAULT_RUNS 

test_files_dir = sys.argv[2] if len(sys.argv) == 3 else "." 


valid_extensions = ['ogv', 'webm', 'mp4', 'mkv', 'avi', 'wms']
testfiles = []
for dirpath, dirname, filenames in os.walk(test_files_dir):
    for filename in filenames:
        testfile_name = "%s" % os.path.abspath(os.path.join(dirpath, filename))
        base_file, extension = os.path.splitext(testfile_name)
        extension = extension.split(".")[-1].lower()
        if extension in valid_extensions:
            testfiles.append(testfile_name)

print testfiles
datestamp = dt.now().strftime("%Y%m%d")
datestamp_dir = "/tmp/%s" % datestamp
if not os.path.exists(datestamp_dir):
    os.mkdir(datestamp_dir)

run = 0
while run < num_runs:
    offset = random.randint(0,len(presets)-1)

    # FIXME : Doesn't support absolute options
    # Take random start/stop values (relative - as percentages)
    start = random.randint(1,100)
    stop = random.randint(1,100)
    if start > stop:
        stop = -1
    start = str(start)
    stop = str(stop)

    # Height, Width, Framerate, Video Bitrate 
    toggle_ht = random.randint(0,1)
    if toggle_ht:
        height = random.randint(40,100)
    toggle_width = random.randint(0,1)
    if toggle_width:
        width = random.randint(40,100)
    toggle_frate = random.randint(0,1)
    if toggle_frate:
        frame_rate = random.randint(20,100)
    toggle_vbrate = random.randint(0,1)
    if toggle_vbrate:
        video_bitrate = random.randint(40,100)

    options = []
    options_str = ""
    if toggle_ht:
        options += ["--height", str(height)]
        options_str += "-h%d" % height
    if toggle_width:
        options += ["--width", str(width)]
        options_str += "-w%d" % width
    if toggle_frate:
        options += ["--framerate", str(frame_rate)]
        options_str += "-f%d" % frame_rate
    if toggle_vbrate:
        options += ["--video-bitrate", str(video_bitrate)]
        options_str += "-b%d" % video_bitrate

    # Determine input/output values
    in_offset = random.randint(0, len(testfiles)-1)
    input = testfiles[in_offset]
    output, ext = os.path.splitext(input) 
    outfile = output.split("/")[-1]
    outfile = "%s-%s-%s%s-%d.%s" % (outfile, start, stop, options_str, \
                                    run, extensions[offset])
    output = os.path.join("/tmp", datestamp, outfile)

    subprocess_array = ["./arista-transcode"] + presets[offset] + \
                        [input, "-o", output, "--start-time", start, \
                         "--stop-time", stop] + options
    print (" ").join(subprocess_array)
    x = subprocess.call(subprocess_array)
    time.sleep(2)
    run += 1
