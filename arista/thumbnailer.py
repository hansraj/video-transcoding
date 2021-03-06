
import os
import sys
import gst
import thread
import gtk
import gtk.gdk
from discoverer import Discoverer
import logging
_log = logging.getLogger("arista.transcoder")

class Thumbnailer(object):
    
    def __init__(self, filepath, output_dir, fileinfo=None, interval=None, number=5,\
                 width=120, height=90, preserve_aspect_ratio=True, prefix="thumbnail", format='jpeg'):
        self.filepath = filepath
        self.output_dir = output_dir
        self.width = width
        #The default scaling is set to 4:3, with 120*90
        self.height = height
        self.count = number
        self.interval = interval
        #TODO: Need to use proper pixel-aspect-ratio
        self.par = preserve_aspect_ratio
        self.prefix = prefix
        self.format = format
        self.fileinfo = fileinfo

    def on_new_preroll_cb(self, appsink):
        buffer = appsink.emit('pull-preroll')
        if buffer:
            self._load_and_save_file(buffer, buffer.timestamp/gst.SECOND)

    def create_thumbnails(self):
        _log.debug("Getting Thumbnails for %s" % self.filepath)

        if not os.path.exists(self.filepath):
            _log.debug("File not found: %s" % self.filepath)
            return False

        if self.fileinfo is None:
            self.fileinfo = Discoverer(self.filepath)
            self.fileinfo.do_discovery()

        if (self.fileinfo is None) or (self.fileinfo.videolength <= 0) or not self.fileinfo.is_video:
            _log.debug("Skipping thumbnail creation. No video stream found for file: %s." % self.filepath)
            return False
 
        offset = counter = 0 
        caps = "video/x-raw-rgb,format=RGB,width=%s,height=%s,pixel-aspect-ratio=1/1" % (self.width, self.height)
        cmd = "uridecodebin uri=file://%s  ! ffmpegcolorspace ! videorate ! videoscale ! " \
                "ffmpegcolorspace ! appsink name=sink caps=%s" % \
                (os.path.abspath(self.filepath), caps)

        pipeline = gst.parse_launch(cmd)
        appsink = pipeline.get_by_name("sink")
        appsink.set_property('emit-signals', True)
        #Set sync off, make decoding faster
        appsink.set_property('sync', False)
        appsink.connect('new-preroll', self.on_new_preroll_cb)
        pipeline.set_state(gst.STATE_PAUSED)
        pipeline.get_state()
    
        if self.interval is None:
            self.interval = ((self.fileinfo.videolength/gst.SECOND) / self.count) or 1
    
        while ((offset < self.fileinfo.videolength/gst.SECOND) and (counter < self.count)):
            ret = pipeline.seek_simple( 
                gst.FORMAT_TIME, gst.SEEK_FLAG_ACCURATE | gst.SEEK_FLAG_FLUSH, offset * gst.SECOND)
            pipeline.get_state()
            offset += self.interval
            counter += 1
        return True

    # Load pixbuf and save file to disk
    def _load_and_save_file(self, buffer, offset):
        file_name = "%s/%s_%s.%s" %  (self.output_dir, self.prefix, offset, self.format)
        try:
            pix_buf = gtk.gdk.pixbuf_new_from_data(buffer.data, \
                        gtk.gdk.COLORSPACE_RGB, False, 8, self.width, self.height, self.width * 3)
            pix_buf.save(file_name, 'jpeg')
        except Exception as e:
            _log.debug("Error saving %s to disk: %s " % (file_name, e))

