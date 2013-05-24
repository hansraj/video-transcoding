"""
Implementation of worker functionality as per new options parameter and using 
only Transcoder object. Do not use arista queues.
"""
import arista
import gobject
import gst

from arista.transcoder import TranscoderOptions, Transcoder
import arista.presets

import os
import requests
import sys

# Do you want to run with Celery integrated?
CELERY_INTEGRATED = True
if CELERY_INTEGRATED:
    import celery.states as task_states
    from celery import current_task

# Cleanup input/output files after transcoding is done

DEFAULT_DEVICE = 'computer'
DEFAULT_PRESET = 'H264-2pass'
LOCAL_INPUT_DIR = '/home/xvid/input'
LOCAL_OUTPUT_DIR = '/home/xvid/output'
OUTPUT_EXT = '.mp4'
CHUNK_SIZE = 1024
MAX_NO_UPDATES = 20
UPDATE_INTERVAL = 1000

class XvidTranscoder(object):
    """ Implements our custom transcoder class."""
    def __init__(self, fe_input):
        self._inurl = fe_input['input']
        # FIXME Validate options
        if 'options' in fe_input:
            self._options = fe_input['options']
        else:
            self._options = {}

        # Required parameters for Transcoder, populated at different positions
        self.input_file = None
        self.output_file = None
        self.preset = None 

        self.is_transcoding = False
        gobject.threads_init()
        self.mainloop = gobject.MainLoop()

        self._no_updates = 0
        self._percent = 0.0
        self.input_info = None
        self.output_info = None

    def get_output_info(self):

        def _got_info(info, is_media):
            if not is_media:
                print is_media
                # Error in discovery of output file
            else:
                self.output_info = info

            print self.input_info.videolength
            print self.output_info.videolength
            # now quit the main loop
            gobject.idle_add(self.mainloop.quit)            
 
        if not self.output_file:
            # BUG (output_file is None or empty and we got transcoding done)
            pass
        output_discoverer = arista.discoverer.Discoverer(self.output_file)
        output_discoverer.connect("discovered", _got_info)
        output_discoverer.discover()
        # main loop is still running. Can close now

    def prepare(self):
        """ Setup Preset. Downloads the file to local filesystem."""
        try:
            # update - preparing state.
            self._update_status(state='PREPARING')
            devices = arista.presets.get()
            device = devices[DEFAULT_DEVICE]
            self.preset = device.presets[DEFAULT_PRESET]
            input_file = self._inurl
            input_file = input_file.split("/")[-1]
            self.input_file = os.path.abspath(os.path.join(LOCAL_INPUT_DIR,
                                                             input_file))

            self._download_file(self.input_file)

            output_file_ext = OUTPUT_EXT
            output_file_name = os.path.splitext(input_file)[0]
            output_file = output_file_name + output_file_ext
            self.output_file = os.path.abspath(os.path.join(LOCAL_OUTPUT_DIR,
                                                             output_file))

        except KeyError as e:
            self.preset = None;self.input_file = None;self.output_uri = None

    def finish(self):
        self._update_status(meta=dict(per_comp=100.0), state='COMPLETING')
        # do upload 
        pass

    def do_transcode(self):
        # FIXME : validations
        opt = TranscoderOptions(uri=self.input_file, 
                                output_uri=self.output_file, 
                                preset=self.preset, 
                                **self._options)
        transcoder = Transcoder(opt)
        transcoder.connect('pass-setup', self._transcoder_pass_setup)
        transcoder.connect('pass-complete', self._transcoder_pass_complete)
        transcoder.connect('complete', self._transcoder_complete)
        transcoder.connect('error', self._transcoder_error)

        self.mainloop.run()

    def do_all(self):
        """ A wrapper function that calls prepare/do_transcode/finish"""
        try:
            self.prepare()
            self.do_transcode()
            self.finish()
        finally:
            print "finally"
            self._do_cleanup()

    def _do_cleanup(self):
        """ Does all finally cleanup"""
        if self.mainloop.is_running():
            self.mainloop.quit()
        try:
            os.unlink(self.input_file)
            os.unlink(self.output_file)
        except:
            pass

    def _download_file(self, dl_file_path):
        x = requests.get(self._inurl, stream=True)
        total_size = int(x.headers['content-length'])
        with open(dl_file_path, 'wb') as dlfile:
            chunks = 0
            for chunk in x.iter_content(CHUNK_SIZE):
                dlfile.write(chunk)
                chunks += 1
                self._dl_percent = chunks * CHUNK_SIZE / float(total_size)
                msg = "%.2f\r" % (self._dl_percent *100)

    def _update_status(self, meta=None, state=None):
        if CELERY_INTEGRATED:
            current_task.update_state(meta=meta, state=state)

    def _status_poller(self, enc, current_pass):
        # When an update from previous pass comes while the first pass is not 
        # ready. POSITION_QUERY fails
        # as such harmless, but throws an exception un-necessarily
        # FIXME : position query will fail for 'very small update interval'
        #         when the pipeline is not yet in PLAYING state
        try:
            percent, remaining = enc.status
        except:
            print "1"
            return True
        # Adjust the reported percent to num-passes
        num_passes = enc.preset.pass_count
        pass_percent = percent / num_passes
        percent = pass_percent + (1.0 * enc.enc_pass / num_passes)

        if self._percent == percent:
            self._no_updates += 1
        else:
            self._no_updates = 0

        if self._no_updates > MAX_NO_UPDATES:
            print self._no_updates
            # encoder hanged
            self.transcoder.stop()
            gobject.idle_add(mainloop.quit)
        self._percent = percent

        percent = percent * 100.0 
        meta=dict(per_comp=percent) 

        self._update_status(meta)
        return (percent < 100.0)
   
    def _transcoder_pass_setup(self, transcoder):
        self.is_transcoding = True
        if transcoder.enc_pass == 0:
            # When the first pass is setup - we already have the input file
            # info or else the pass won't be setup
            self.input_info = transcoder.info
            meta = dict(per_comp=0,worker_id=current_task.request.hostname)
            self._update_status(meta=meta, state=task_states.STARTED)
        ret = gobject.timeout_add(UPDATE_INTERVAL, self._status_poller, transcoder, 
                                  transcoder.enc_pass)

    def _transcoder_pass_complete(self, transcoder):
        self.is_transcoding = False
    
    def _transcoder_complete(self, transcoder):
        transcoder.stop()
        self.get_output_info()
    
    def _transcoder_error(transcoder):
        gobject.idle_add(self.mainloop.quit)

if __name__ == '__main__':
    fe_input = {'input_directive':\
                'https://s3.amazonaws.com/xvid-mediacoder/sintel_trailer-480p.ogv',
                'options':{'start_time':10,
                           'height':80,
                           'width':80,
                           'framerate':100,
                           }
                }
    xvid_transcoder = XvidTranscoder(fe_input)
    xvid_transcoder.do_all()
    """finally:
        if xvid_transcoder is not None:
            if xvid_transcoder.mainloop.is_running():
                xvid_transcoder.mainloop.quit()
            try:
                # Cleanup input and output file. Ignore any errors
                if xvid_transcoder.input_file:
                    os.unlink(xvid_transcoder.input_file)
                if xvid_transcoder.output_file:
                    os.unlink(xvid_transcoder.output_file)
            except:
                    pass"""
