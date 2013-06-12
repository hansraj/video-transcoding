
import os
import sys
import gst
import thread
import gtk
import gtk.gdk

import logging
_log = logging.getLogger("arista.transcoder")

class Thumbnailer(object):
    
    def __init__(self, filepath, output_dir, width=120, height=90, count=5, interval=10, par=1):
        self.filepath = filepath
        self.output_dir = output_dir
        self.width = width
        #The default scaling is set to 4:3, with 120*90
        self.height = height
        self.count = count
        self.interval = interval
        #TODO: Need to use proper pixel-aspect-ratio
        self.par = par

    def create_thumbnails(self):
        _log.debug("Getting Thumbnails for %s" % self.filepath)
        offset = counter = 0 
        caps = "video/x-raw-rgb,format=RGB,width=%s,height=%s,pixel-aspect-ratio=1/1" % (self.width, self.height)
        cmd = "uridecodebin uri=file://%s  ! ffmpegcolorspace ! videorate ! videoscale ! " \
                "ffmpegcolorspace ! appsink name=sink caps=%s" % \
                (os.path.abspath(self.filepath), caps)

        pipeline = gst.parse_launch(cmd)
        appsink = pipeline.get_by_name("sink")
        pipeline.set_state(gst.STATE_PAUSED)
        pipeline.get_state()
    
        length, format = pipeline.query_duration(gst.FORMAT_TIME)
        while offset < length/gst.SECOND and counter < self.count:
            ret = pipeline.seek_simple( 
                gst.FORMAT_TIME, gst.SEEK_FLAG_ACCURATE | gst.SEEK_FLAG_FLUSH, offset * gst.SECOND)
            buffer = appsink.emit('pull-preroll')
            if buffer:
                self._load_and_save_file(offset, buffer)
                offset += self.interval
            counter += 1
        return True

    # Load pixbuf and save file to disk
    def _load_and_save_file(self, offset, buffer):
        file_name = "%s/thumbnail_%s.jpeg" %  (self.output_dir, offset)
        try:
            pix_buf = gtk.gdk.pixbuf_new_from_data(buffer.data, \
                        gtk.gdk.COLORSPACE_RGB, False, 8, self.width, self.height, self.width * 3)
            pix_buf.save(file_name, 'jpeg')
        except Exception as e:
            _log.debug("Error saving %s to disk: %s " % (file_name, e))

