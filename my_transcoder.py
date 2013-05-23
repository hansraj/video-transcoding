
import arista
import gobject
import gst

from arista.transcoder import TranscoderOptions, Transcoder
import arista.presets

import os
import requests

DEFAULT_DEVICE = 'computer'
DEFAULT_PRESET = 'H264-2pass'
LOCAL_INPUT_DIR = '.'
LOCAL_OUTPUT_DIR = '.'
OUTPUT_EXT = '.mp4'

class XvidTranscoder(object):
    """ Implements our custom transcoder class."""

    def __init__(self, fe_input):
fe_input = {'input_directive':'https://s3.amazonaws.com/xvid-mediacoder/sintel_trailer-480p.ogv',
            'options':{'start_time':10,
                       'height':80,
                       'width':80,
                       'framerate':100,
                       }
            }
try:
    devices = arista.presets.get()
    device = devices[DEFAULT_DEVICE]
    preset = device.presets[DEFAULT_PRESET]
except KeyError as e: # Config Error somewhere 
    print e.message

try:
    input_file = fe_input['input_directive']
    input_file = input_file.split("/")[-1]
    input_file_path = os.path.abspath(os.path.join(LOCAL_INPUT_DIR, input_file))
    output_file_ext = OUTPUT_EXT
    output_file_name = os.path.splitext(input_file)[0]
    output_file = output_file_name + output_file_ext
    output_file_path = os.path.abspath(os.path.join(LOCAL_OUTPUT_DIR, output_file))
except KeyError as e: # Required parameter not passed
    print e.message

options = fe_input['options'] if 'options' in fe_input else {}
opt = TranscoderOptions(uri=input_file_path, output_uri=output_file_path, preset=preset, **options)
print "uri:", opt.uri, "output_uri:", opt.output_uri, opt.start_time, opt.stop_time, opt.height, opt.width, opt.framerate, opt.video_bitrate
transcoder = Transcoder(opt)
print transcoder

is_transcoding = False 

def _print_status(enc):
    if not is_transcoding:
        return True
    print enc
    print enc.status
    return True

def _transcoder_pass_setup(transcoder):
    global is_transcoding
    is_transcoding = True
    ret = gobject.timeout_add(1000, _print_status, transcoder)
    print "pass-setup"

def _transcoder_pass_complete(transcoder):
    global is_transcoding
    is_transcoding = False
    print "pass-complete"

def _transcoder_complete(transcoder):
    mainloop.quit()
    print "complete"

def _transcoder_error(transcoder):
    mainloop.quit()
    print "error"

transcoder.connect('pass-setup', _transcoder_pass_setup)
transcoder.connect('pass-complete', _transcoder_pass_complete)
transcoder.connect('complete', _transcoder_complete)
transcoder.connect('error', _transcoder_error)


mainloop = gobject.MainLoop()
mainloop.run()


