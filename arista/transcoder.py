#!/usr/bin/env python

"""
    Arista Transcoder
    =================
    A class to transcode files given a preset.
    
    License
    -------
    Copyright 2009 - 2011 Daniel G. Taylor <dan@programmer-art.org>
    
    This file is part of Arista.

    Arista is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as 
    published by the Free Software Foundation, either version 2.1 of
    the License, or (at your option) any later version.

    Arista is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with Arista.  If not, see
    <http://www.gnu.org/licenses/>.
"""

import gettext
import logging
import os
import os.path
import sys
import time
import threading
import random
# Default to 1 CPU
try:
    import multiprocessing
    try:
        CPU_COUNT = multiprocessing.cpu_count()
    except NotImplementedError:
        pass
except ImportError:
    pass
CPU_COUNT = 1 

import gobject
import gst
import gtk
import gtk.gdk

import discoverer

from threading import Thread
_ = gettext.gettext
_log = logging.getLogger("arista.transcoder")

# =============================================================================
# Custom exceptions
# =============================================================================

_NO_APPLICATION_MSG_TIMEOUT = 20000

class TranscoderException(Exception):
    """
        A generic transcoder exception to be thrown when something goes wrong.
    """
    pass

class TranscoderStatusException(TranscoderException):
    """
        An exception to be thrown when there is an error retrieving the current
        status of an transcoder.
    """
    pass

class PipelineException(TranscoderException):
    """
        An exception to be thrown when the transcoder fails to construct a 
        working pipeline for whatever reason.
    """
    pass

# =============================================================================
# Transcoder Options
# =============================================================================

class TranscoderOptions(object):
    """
        Options pertaining to the input/output location, presets, 
        subtitles, etc.
    """
    def __init__(self, uri = None, preset = None, output_uri = None, ssa = False,
                 subfile = None, subfile_charset = None, font = "Sans Bold 16",
                 deinterlace = None, crop = None, title = None, chapter = None,
                 audio = None, start_time = 0, stop_time = -1, nb_threads = 0,
                 height = None, width = None, framerate = None,
                 video_bitrate = None, absolute = False, max_duration = None,
                 thumbnail_offset = 0, **kw):
        """
            @type uri: str
            @param uri: The URI to the input file, device, or stream
            @type preset: Preset
            @param preset: The preset to convert to
            @type output_uri: str
            @param output_uri: The URI to the output file, device, or stream
            @type subfile: str
            @param subfile: The location of the subtitle file
            @type subfile_charset: str
            @param subfile_charset: Subtitle file character encoding, e.g.
                                    'utf-8' or 'latin-1'
            @type font: str
            @param font: Pango font description
            @type deinterlace: bool
            @param deinterlace: Force deinterlacing of the input data
            @type crop: int tuple
            @param crop: How much should be cropped on each side
                                    (top, right, bottom, left)
            @type title: int
            @param title: DVD title index
            @type chatper: int
            @param chapter: DVD chapter index
            @type audio: int
            @param audio: DVD audio stream index

            @type start_time: int
            @param start_time: Seek start position. 
            @type stop_time: int
            @param stop_time: Seek end position

            @type nb_threads: int
            @param nb_threads: Number of threads to use
        """
        _log.debug("unhandled options : %s" % kw)
        self.reset(uri, preset, output_uri, ssa,subfile, subfile_charset, font, deinterlace,
                   crop, title, chapter, audio, start_time, stop_time, nb_threads,
                   height, width, framerate, video_bitrate, absolute, max_duration, 
                   thumbnail_offset)
    
    def reset(self, uri = None, preset = None, output_uri = None, ssa = False,
              subfile = None, subfile_charset = None, font = "Sans Bold 16",
              deinterlace = None, crop = None, title = None, chapter = None,
              audio = None, start_time = 0, stop_time = -1, nb_threads = 0,
              height = None, width = None, framerate = None,
              video_bitrate = None, absolute = False, max_duration = None,
              thumbnail_offset = 0):
        """
            Reset the input options to nothing.
        """
        self.uri = uri
        self.preset = preset
        self.output_uri = output_uri
        self.ssa = ssa
        self.subfile = subfile
        self.subfile_charset = subfile_charset
        self.font = font
        self.deinterlace = deinterlace
        self.crop = crop
        self.title = title
        self.chapter = chapter
        self.audio = audio
        try:
            self.start_time = int(start_time)
        except (TypeError, ValueError):
            _log.debug("invalid start_time sent %s" % start_time)
            self.start_time = 0
        try:
            self.stop_time = int(stop_time)
        except (TypeError, ValueError):
            _log.debug("invalid stop_time sent %s" % stop_time)
            self.stop_time = -1
        self.nb_threads = nb_threads
        self.height = height
        self.width = width
        self.framerate = framerate
        self.video_bitrate = video_bitrate
        self.absolute = absolute
        self.max_duration = max_duration
        self.thumbnail_offset = thumbnail_offset

# =============================================================================
# The Transcoder
# =============================================================================

class Transcoder(gobject.GObject):
    """
        The transcoder - converts media between formats.
    """
    __gsignals__ = {
        "discovered": (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE,
                      (gobject.TYPE_PYOBJECT,      # info
                       gobject.TYPE_PYOBJECT)),    # is_media
        "pass-setup": (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE, tuple()),
        "pass-complete": (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE, tuple()),
        "message": (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE,
                   (gobject.TYPE_PYOBJECT,         # bus
                    gobject.TYPE_PYOBJECT)),       # message
        "complete": (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE, tuple()),
        "error": (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE,
                 (gobject.TYPE_PYOBJECT,  # error 
                  gobject.TYPE_PYOBJECT)),# error_num
    }
    
    def __init__(self, options):
        """
            @type options: TranscoderOptions
            @param options: The options, like input uri, subtitles, preset, 
                            output uri, etc.
        """
        self.__gobject_init__()
        self.options = options
        
        self.pipe = None
        
        self.enc_pass = 0
        self.random_num = str(time.time()) + "-" +  str(random.randint(1,100000))
       
        if self.options.nb_threads == 0: # auto-detect
            self.cpu_count = CPU_COUNT
        else:
            self.cpu_count = self.options.nb_threads

        self._percent_cached = 0
        self._percent_cached_time = 0
        
        self.do_discovery(options.uri, self._got_info)
  
        self.output_duration = 0.0
        self._lock = threading.Lock()

    def _got_info(self, info, is_media):
        self.info = info
        self.emit("discovered", info, is_media)
        
        if info.is_video or info.is_audio:
            try:
                self._setup_pass()
            except PipelineException, e:
                self.emit("error", str(e), 0)
                return
            self.pause()

    def do_discovery(self, filename, callback):
        """ Does discovery of the filename and connects the discovered signal to callback"""
        if not filename:
            # BUG 
            return
        if not hasattr(callback, '__call__'):
            # FIXME : what if passed callback is a class?
            # BUG 
            return
        d = discoverer.Discoverer(filename)
        d.connect("discovered", callback)
        d.discover()

    @property
    def infile(self):
        """
            Provide access to the input uri for backwards compatibility after
            moving to TranscoderOptions for uri, subtitles, etc.
            
            @rtype: str
            @return: The input uri to process
        """
        return self.options.uri
    
    @property
    def preset(self):
        """
            Provide access to the output preset for backwards compatibility
            after moving to TranscoderOptions.
            
            @rtype: Preset
            @return: The output preset
        """
        return self.options.preset
   
    def _get_source(self):
        """
            Return a file or dvd source string usable with gst.parse_launch.
            
            This method uses self.infile to generate its output.
            
            @rtype: string
            @return: Source to prepend to gst-launch style strings.
        """
        # FIXME : Not dealing with http source in transcoder yet
        if self.infile.startswith("file://"):
            filename = self.infile
        else:
            filename = "file://" + os.path.abspath(self.infile)
            
        return "uridecodebin uri=\"%s\" name=uridecode" % filename
    
    def _get_container(self):
        container = None
        if self.info.is_video and self.info.is_audio:
            container = self.preset.container
        elif self.info.is_video:
            container = self.preset.vcodec.container and \
                        self.preset.vcodec.container or \
                        self.preset.container
        elif self.info.is_audio:
            container = self.preset.acodec.container and \
                        self.preset.acodec.container or \
                        self.preset.container
        return container
    
    def _update_preset_to_vencoder_limits(self):
        if not self.info.is_video:
            _log.debug("Videotrack Not present. We shouldn't Come here. BUG()")
            return

        if not self.preset.vcodec:
            _log.debug("No Videocodec defined in Preset. We shouldn't come" \
                       " here. BUG()")
            return 

        # =================================================================
        # Update limits based on what the encoder really supports
        # =================================================================
        element = gst.element_factory_make(self.preset.vcodec.name, "vencoder")
            
        # TODO: Add rate limits based on encoder sink below
        for cap in element.get_pad("sink").get_caps():
            for field in ["width", "height"]:
                if cap.has_field(field):
                    value = cap[field]
                    if isinstance(value, gst.IntRange):
                        vmin, vmax = value.low, value.high
                    else:
                        vmin, vmax = value, value
                    
                    cur = getattr(self.preset.vcodec, field)
                    if cur[0] < vmin:
                        cur = (vmin, cur[1])
                        setattr(self.preset.vcodec, field, cur)
                
                    if cur[1] > vmax:
                        cur = (cur[0], vmax)
                        setattr(self.preset.vcodec, field, cur)

    def _setup_pixel_aspect_ratio(self):
        # =================================================================
        # Properly handle and pass through pixel aspect ratio information
        # =================================================================
        for x in range(self.info.videocaps.get_size()):
            struct = self.info.videocaps[x]
            if struct.has_field("pixel-aspect-ratio"):
                # There was a bug in xvidenc that flipped the fraction
                # Fixed in svn on 12 March 2008
                # We need to flip the fraction on older releases!
                par = struct["pixel-aspect-ratio"]
                if self.preset.vcodec.name == "xvidenc":
                    for p in gst.registry_get_default().get_plugin_list():
                        if p.get_name() == "xvid":
                            if p.get_version() <= "0.10.6":
                                par.num, par.denom = par.denom, par.num
                for vcap in self.vcaps:
                    vcap["pixel-aspect-ratio"] = par
                break

        # FIXME a bunch of stuff doesn't seem to like pixel aspect ratios
        # Just force everything to go to 1:1 for now...
        for vcap in self.vcaps:
            vcap["pixel-aspect-ratio"] = gst.Fraction(1, 1)
            

    def _validate_and_update_resolution(self):
        # =================================================================
        # Calculate video width/height, crop and add black bars if necessary
        # =================================================================

        input_width = self.info.videowidth
        input_height = self.info.videoheight

        if self.options.height is not None: # user specified height
            if self.options.absolute:
                output_height = self.options.height
            else: # percentage
                output_height = int(input_height * (self.options.height / 100.0))
        else:
            output_height = input_height
        
        if self.options.width is not None:
            if self.options.absolute:
                output_width = self.options.width
            else:
                output_width = int(input_width * (self.options.width / 100.0))
        else:
            output_width = input_width

        vcrop = ""
        crop = [0, 0, 0, 0]
        if self.options.crop:
            crop = self.options.crop
            vcrop = "videocrop top=%i right=%i bottom=%i left=%i ! "  % \
                     (crop[0], crop[1], crop[2], crop[3])

        owidth = output_width - crop[1] - crop[3]
        oheight = output_height - crop[0] - crop[2]
       
        wmin, wmax = self.preset.vcodec.width
        hmin, hmax = self.preset.vcodec.height
            
        rel_or_abs = "(relative)" if not self.options.absolute else ""
        _log.debug("video input::height: %d, width : %d" % \
                        (input_height, input_width))
        if self.options.height is not None:
            _log.debug("user input::height: %d %s" % \
                        (self.options.height, rel_or_abs))
        if self.options.width is not None:
            _log.debug("user input::width: %d %s" % \
                        (self.options.width, rel_or_abs))
        
        _log.debug("crop::top:%d, right:%d,bottom:%d, left:%d"  % \
                    tuple(crop))
        _log.debug("determined (crop adjusted) height:%d, width :%d" % \
                        (oheight, owidth))

        try:
            if self.info.videocaps[0].has_key("pixel-aspect-ratio"):
                owidth = int(owidth * float(self.info.videocaps[0]["pixel-aspect-ratio"]))
        except KeyError:
            # The videocaps we are looking for may not even exist, just ignore
            pass
            

        width, height = owidth, oheight
        # =================================================================
        # Modifiying height and width with user supplied values. (Hansraj)
        # =================================================================
        # Scale width / height to fit requested min/max
        # FIXME : Why the scaling below is required. Need to check
        if width < wmin:
            width = wmin
            #height = int((float(wmin) / owidth) * oheight)
        elif width > wmax:
            width = wmax
            #height = int((float(wmax) / owidth) * oheight)
            
        if height < hmin:
            height = hmin
            #width = int((float(hmin) / oheight) * owidth)
        elif height > hmax:
            height = hmax
            #width = int((float(hmax) / oheight) * owidth)

        # Add any required padding
        # TODO: Remove the extra colorspace conversion when no longer
        #       needed, but currently xvidenc and possibly others will fail
        #       without it!
        vbox = ""
        if width < wmin and height < hmin:
            wpx = (wmin - width) / 2
            hpx = (hmin - height) / 2
            vbox = "videobox left=%i right=%i top=%i bottom=%i ! ffmpegcolorspace ! " % \
                   (-wpx, -wpx, -hpx, -hpx)
        elif width < wmin:
            px = (wmin - width) / 2
            vbox = "videobox left=%i right=%i ! ffmpegcolorspace ! " % \
                   (-px, -px)
        elif height < hmin:
            px = (hmin - height) / 2
            vbox = "videobox top=%i bottom=%i ! ffmpegcolorspace ! " % \
                   (-px, -px)
        
        # FIXME Odd widths / heights seem to freeze gstreamer
        if width % 2:
            width += 1
        if height % 2:
            height += 1
         
        _log.debug("determined (preset adjusted) height:%d, width :%d" % \
                        (height, width))

        for vcap in self.vcaps:
            vcap["width"] = width
            vcap["height"] = height

        return vcrop, vbox
         
    def _setup_video_framerate(self):
        # =================================================================
        # Setup video framerate 
        # =================================================================

        # FIXME : Not working for webm yet
        num = self.info.videorate.num
        denom = self.info.videorate.denom

        input_num = num
        input_denom = denom
        _log.debug("Input video framerate : %d/%d", num, denom)
        if self.options.framerate is not None:
            if self.options.absolute:
                num = self.options.framerate
                denom = 1 #Considering the the denom to 1
            else:
                
                num = round(num * self.options.framerate/100)
                # don't scale denom as well!! 

        _log.debug("Framerate before Preset Comparison: %d/%d" % (num, denom))
        rmin = self.preset.vcodec.rate[0].num / \
                   float(self.preset.vcodec.rate[0].denom)
        rmax = self.preset.vcodec.rate[1].num / \
                   float(self.preset.vcodec.rate[1].denom)
        orate = num / float(denom)
            
        if orate > rmax:
            num = self.preset.vcodec.rate[1].num
            denom = self.preset.vcodec.rate[1].denom
        elif orate < rmin:
            num = self.preset.vcodec.rate[0].num
            denom = self.preset.vcodec.rate[0].denom

        _log.debug("Final Determined Framerate : %d/%d" % (num, denom))
        return num, denom    

    def _update_preset_to_aencoder_limits(self):
        # =================================================================
        # Update limits based on what the encoder really supports
        # =================================================================
        if not self.info.is_audio:
            _log.debug("There's no audio track. This part should not be called" \
                        "BUG()")
        element = gst.element_factory_make(self.preset.acodec.name, "aencoder")
            
        fields = {}
        for cap in element.get_pad("sink").get_caps():
            for field in ["width", "depth", "rate", "channels"]:
                if cap.has_field(field):
                    if field not in fields:
                        fields[field] = [0, 0]
                    value = cap[field]
                    if isinstance(value, gst.IntRange):
                        vmin, vmax = value.low, value.high
                    else:
                        vmin, vmax = value, value
                    
                    if vmin < fields[field][0]:
                        fields[field][0] = vmin
                    if vmax > fields[field][1]:
                        fields[field][1] = vmax
        
        for name, (amin, amax) in fields.items():
            cur = getattr(self.preset.acodec, field)
            if cur[0] < amin:
                cur = (amin, cur[1])
                setattr(self.preset.acodec, field, cur)
            if cur[1] > amax:
                cur = (cur[0], amax)
                setattr(self.preset.acodec, field, cur)
        
    def _get_input_video_bitrate(self):
        filesize = 0
        oabitrate = 0
        try:
            filesize = os.path.getsize( os.path.abspath(self.infile) )
        except:
            _log.debug(_("Error reading FILESIZE for %(filename)s") % { "filename": self.options.uri})
            return 0
        # FIXME: Currently the audio bitrate value obtained is unreliable, so not using it for the calculation.
        # Uncomment the 2 lines below to use the value.
        #if self.info.tags.has_key("bitrate"):
        #     oabitrate = self.info.tags["bitrate"]

        # Using formula as : Video bitrate (Kb/s) = Filesize (Kb) / length (sec) - (audio-bitrate)
        # FIXME : is it kbps
        #return (filesize/(self.info.videolength/gst.SECOND) - oabitrate)
        
        # calculating in kbps for H.264. Note vp8enc requires it in bits per second 
        fsize_bits = filesize * 8 
        fsize_bps = fsize_bits/(self.info.videolength/gst.SECOND) 
        fsize_kbps = fsize_bps/1000
        return fsize_kbps


    def _set_video_bitrate(self):

        if self.options.video_bitrate and self.options.absolute:
            return self.options.video_bitrate

        target_bitrate = self._get_input_video_bitrate()
        if self.options.absolute:
            return int(target_bitrate)

        # Both are false now
        if self.options.video_bitrate: # relative
            target_bitrate *= self.options.video_bitrate / 100.0

        return int(target_bitrate)

    def _setup_subtitles_from_file(self):
        sub = ""
        cmd = ""
        if self.options.subfile and self.options.start_time == 0:
            charset = ""
            if self.options.subfile_charset:
                charset = "subtitle-encoding=\"%s\"" % \
                                            self.options.subfile_charset
            
            # Render subtitles onto the video stream
            sub = "textoverlay font-desc=\"%(font)s\" name=txt ! " % {
                "font": self.options.font,
            }
            cmd += " filesrc location=\"%(subfile)s\" ! subparse " \
                         "%(subfile_charset)s ! txt." % {
                         "subfile": self.options.subfile,
                         "subfile_charset": charset,
            }
        elif self.options.subfile:
            _log.debug(_("Subtitles not supported in combination with seeking."))

        if self.options.ssa is True and self.options.start_time == 0:             
            # Render subtitles onto the video stream
            sub = "textoverlay font-desc=\"%(font)s\" name=txt ! " % {
                "font": self.options.font,
            }
            cmd += " filesrc location=\"%(infile)s\" ! matroskademux name=demux ! ssaparse ! txt. " % {
                "infile": self.infile,
            }
        elif self.options.ssa is True:
            _log.debug(_("Subtitles not supported in combination with seeking."))
        
        return cmd, sub

    def _get_watermark_text(self):
        overlay = ""
        if self.options.text_overlay:
            overlay = "! textoverlay font-desc=\"%s\" text=\"%s\" " % \
                    (self.options.font, self.options.text_overlay)

        return overlay

    def _setup_pass(self):
        """
            Setup the pipeline for an encoding pass. This configures the
            GStreamer elements and their setttings for a particular pass.
        """
        # Get limits and setup caps
        self.vcaps = gst.Caps()
        self.vcaps.append_structure(gst.Structure("video/x-raw-yuv"))
        self.vcaps.append_structure(gst.Structure("video/x-raw-rgb"))
        
        self.acaps = gst.Caps()
        self.acaps.append_structure(gst.Structure("audio/x-raw-int"))
        self.acaps.append_structure(gst.Structure("audio/x-raw-float"))
        
        # =====================================================================
        # Setup video, audio/video, or audio transcode pipeline
        # =====================================================================
        
        # Figure out which mux element to use
        container = self._get_container()

        mux_str = ""
        if container:
            mux_str = "%s name=mux ! queue !" % container
        
        # Decide whether or not we are using a muxer and link to it or just
        # the file sink if we aren't (for e.g. mp3 audio)
        if mux_str:
            premux = "mux."
        else:
            premux = "sink."
        
        uridecode_str = self._get_source()
        
        mux_str = "%s filesink name=sink " \
                  "location=\"%s\"" % (mux_str, self.options.output_uri)
        
        video_str = ""    
        if self.info.is_video and self.preset.vcodec:
            # Ensure that preset limits fall within 'actual encoder limits'
            self._update_preset_to_vencoder_limits()

            # Validate the height and width to preset
            vcrop,vbox = self._validate_and_update_resolution()
            
            # =================================================================
            # Setup video framerate and add to caps
            # =================================================================
            num, denom = self._setup_video_framerate() 
            for vcap in self.vcaps:
                vcap["framerate"] = gst.Fraction(num, denom)

            # =================================================================
            # Properly handle and pass through pixel aspect ratio information
            # =================================================================
            self._setup_pixel_aspect_ratio() 

            # =================================================================
            # Setup the video encoder and options
            # =================================================================
            vencoder = "%s %s" % (self.preset.vcodec.name,
                                  self.preset.vcodec.passes[self.enc_pass] % {
                                    "random": self.random_num,
                                  })
            
            # FIXME : vp8enc requires the parameter as 'target-bitrate' and 
            # requires it in bps, while x264 requires it in kbps. In general 
            # this handling should be much better than what it is
            target_bitrate = self._set_video_bitrate()
            vencoder += " bitrate={0}".format(target_bitrate)

            deint = ""
            if self.options.deinterlace:
                deint = " ffdeinterlace ! "
            
            transform = ""
            if self.preset.vcodec.transform:
                transform = self.preset.vcodec.transform + " ! "
            
            # FIXME : Not merged subtitles handling from Hansraj's code yet
            cmd, sub = self._setup_subtitles_from_file()
            video_str += cmd

            video_str += " queue name=q_dec_venc_%d " + " ! ffmpegcolorspace ! videorate !" \
                   "%s %s %s %s videoscale ! %s ! %s%s ! tee " \
                   "name=videotee" % \
                   (deint, vcrop, transform, sub, self.vcaps.to_string(), vbox,
                    vencoder)
            video_str += " ! queue name=q_venc_mux_%d "

            _log.debug(video_str)

        # Handle the audio part here. Note we deal with audio only for the last
        # pass
        audio_str = "" 
        if self.info.is_audio and self.preset.acodec and \
           self.enc_pass == len(self.preset.vcodec.passes) - 1:
            # =================================================================
            # Update limits based on what the encoder really supports
            # =================================================================
            self._update_preset_to_aencoder_limits() 
            # =================================================================
            # Prepare audio capabilities
            # =================================================================
            for attribute in ["width", "depth", "rate", "channels"]:
                current = getattr(self.info, "audio" + attribute)
                amin, amax = getattr(self.preset.acodec, attribute)
                
                for acap in self.acaps:
                    if amin < amax:
                        acap[attribute] = gst.IntRange(amin, amax)
                    else:
                        acap[attribute] = amin
            
            # =================================================================
            # Add audio transcoding pipeline to command
            # =================================================================
            aencoder = self.preset.acodec.name + " " + \
                       self.preset.acodec.passes[ \
                            len(self.preset.vcodec.passes) - \
                            self.enc_pass - 1 \
                       ] % {
                            "threads": self.cpu_count,
                       }
            
            audio_str += " queue name=q_dec_aenc_%d" + " ! audioconvert ! " \
                         "audiorate tolerance=100000000 ! " \
                         "audioresample ! %s ! %s " % \
                         (self.acaps.to_string(), aencoder)
            audio_str += " ! queue name=q_aenc_mux_%d"

            _log.debug(audio_str) 

        # =====================================================================
        # Build the pipeline and get ready!
        # =====================================================================

        self._start_ns = 0
        self.counter = 1
        self.prerolled = False

        self.video_str = video_str
        self.audio_str = audio_str
        self.mux_str = mux_str

        self._timeoutid = None # Need to make sure this is None every pass
        self._timeoutid = gobject.timeout_add(_NO_APPLICATION_MSG_TIMEOUT,
                                                self._cb_no_app_message_timeout)
        self._build_pipeline(uridecode_str)
    
    def _build_pipeline(self, uridecode_str):
        """
            Build a gstreamer pipeline from a given gst-launch style string and
            connect a callback to it to receive messages.
            
            @type uridecode_str: string
            @param uridecode_str: A gst-launch string to construct a decode pipeline from.
            @type mux_str: string
            @param mux_str: A gst-launch string to construct a muxer sub-pipeline from.
            @type video_str: string
            @param video_str: A gst-launch string to construct a video sub-pipeline from.
            @type audio_str: string
            @param audio_str: A gst-launch string to construct a audio sub-pipeline from.
        """
        try:
            self.pipe = gst.parse_launch(uridecode_str + " ! fakesink name = fake") # We need a fakesink or uridecodebin won't seek
        except gobject.GError, e:
            raise PipelineException(_("Unable to construct pipeline! ") + \
                                    str(e))

        bus = self.pipe.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_message)

        uridecode_elem = self.pipe.get_by_name("uridecode")
        uridecode_elem.connect("pad-added", self._cb_uridecode_pad_added)
        uridecode_elem.connect("no-more-pads", self._cb_uridecode_no_more_pads)

    def _dec_counter(self):
        self._lock.acquire()
        self.counter -= 1
        counter = self.counter
        self._lock.release()
        if counter == 0:
            self.prerolled = True
            m = gst.message_new_application(self.pipe, 
                                            gst.Structure("Prerolled"))
            self.pipe.post_message(m)

    def _cb_pad_blocked(self, pad, is_blocked):
        if self.prerolled :
            _log.debug("already prerolled")
            return
        self._dec_counter()

    def _cb_uridecode_pad_added(self, elem, pad):
        pad.set_blocked_async(True, self._cb_pad_blocked) 
        self._lock.acquire()
        self.counter += 1
        self._lock.release()

    def _cb_uridecode_no_more_pads(self, elem):
        self._dec_counter()

    def _handle_video_pad_added(self, elem, pad, video_pads):
        if not self.video_str:
            return False

        video_str = self.video_str % (video_pads, video_pads)
        video_subpipe = gst.parse_launch(video_str)

        if video_subpipe:
            video_subpipe.set_state(gst.STATE_PAUSED)
            self.pipe.add(video_subpipe)
            _log.debug("Adding %s to pipeline " % video_subpipe)

            vq = self.pipe.get_by_name("q_dec_venc_%d" % video_pads)
            link = elem.link(vq)
            _log.debug("Result of linking %s to % s => %r" % (elem, vq, link))

            muxer = self.pipe.get_by_name("mux")
            q = self.pipe.get_by_name("q_venc_mux_%d" % video_pads)
            link = q.link(muxer)
            _log.debug("Result of linking %s to % s => %r" % (q, muxer, link))

            return True
        else:
            return False

    def _handle_audio_pad_added(self, elem, pad, audio_pads):
        if not self.audio_str:
            return False

        audio_str = self.audio_str % (audio_pads, audio_pads)
        audio_subpipe = gst.parse_launch(audio_str)

        if audio_subpipe:
            audio_subpipe.set_state(gst.STATE_PAUSED)
            self.pipe.add(audio_subpipe)
            _log.debug("Adding %s to pipeline " % audio_subpipe)

            aq = self.pipe.get_by_name("q_dec_aenc_%d" % audio_pads)
            link = elem.link(aq)
            _log.debug("Result of linking %s to % s => %r" % (elem, aq, link))
            
            muxer = self.pipe.get_by_name("mux")
            q = self.pipe.get_by_name("q_aenc_mux_%d" % audio_pads)
            link = q.link(muxer)
            _log.debug("Result of linking %s to % s => %r" % (q, muxer, link))
            return True
        else:
            return False

    def _do_seek(self, elem):
        start, stop = self.options.start_time, self.options.stop_time
        duration = max(self.info.videolength, self.info.audiolength)
        duration = duration / gst.SECOND

        if start < 0 or stop < -1:
            _log.debug("Start(%d) or Stop(%d) time is invalid" % \
                        (start, stop))
            self.output_duration = duration
            return False

        if stop > -1  and start > stop:
            _log.debug("start(%d) is greater than stop(%d)" % \
                        (start, stop))
            self.output_duration = duration
            return False

        if stop == -1:
            stop_seek_type = gst.SEEK_TYPE_NONE
        else:
            stop_seek_type = gst.SEEK_TYPE_SET
        
        # support for relative seek
        if not self.options.absolute:
            if start > 100.0 or stop > 100.0:
                _log.debug("Relative Duration and start/stop are > 100")
                return False
            if stop != -1:
                stop = duration * stop / 100.0 
            start = duration * start / 100.0

        #Restrict transcoding to max-duration
        if self.options.max_duration:
            if (stop - start) > self.options.max_duration:
                stop -= (stop - start) - self.options.max_duration

        if stop == -1:
            self.output_duration = duration - start 
        else:
            self.output_duration = stop - start
        
        self._start_ns = start * gst.SECOND
                
        seek_flags = gst.SEEK_FLAG_FLUSH | gst.SEEK_FLAG_ACCURATE

        _log.debug("start: %d", start * gst.SECOND)
        _log.debug("stop: %d", stop * gst.SECOND)
        ret = elem.seek(1.0, gst.FORMAT_TIME, seek_flags,
                        gst.SEEK_TYPE_SET, start * gst.SECOND,
                        stop_seek_type, stop * gst.SECOND)
        return ret

    def _on_message(self, bus, message):
        """
            Process pipe bus messages, e.g. start new passes and emit signals
            when passes and the entire encode are complete.
            
            @type bus: object
            @param bus: The session bus
            @type message: object
            @param message: The message that was sent on the bus
        """
        t = message.type
        if t == gst.MESSAGE_EOS:
            self.state = gst.STATE_NULL
            self.emit("pass-complete")
            if self.enc_pass < self.preset.pass_count - 1:
                self.enc_pass += 1
                self._setup_pass()
                self.pause()
            else:
                self.emit("complete")
        elif t == gst.MESSAGE_APPLICATION:
            msg_name = message.structure.get_name()
            if msg_name == "Prerolled":
                _log.debug("Prerolled application msg received!")

                # Do a seek 
                try:
                    if self._do_seek(self.pipe) != True:
                        _log.debug("Seek failed!")
                        # it's better to unblock pads here and emit an error
                        for pad in uridecode_elem.pads():
                            pad.set_blocked_async(False, self._cb_unblocked)
                        self.emit("error", e.message, 0)
                        # failure to seek is an error, that should be raised

                    uridecode_elem = self.pipe.get_by_name("uridecode")
                    fake = self.pipe.get_by_name("fake")

                    self.pipe.remove(fake)
                    fake.set_state(gst.STATE_NULL)

                    # adding muxer sub-pipe                
                    mux_subpipe = gst.parse_launch(self.mux_str)
                    mux_subpipe.set_state(gst.STATE_PAUSED)
                    self.pipe.add(mux_subpipe)

                    # adding and connecting the audio and video sub-pipes
                    video_pads = 0
                    audio_pads = 0
                    for pad in uridecode_elem.pads():
                        if "video" in pad.get_caps().to_string():
                            vpad_added = self._handle_video_pad_added(uridecode_elem, pad, video_pads)
                            if vpad_added:
                                video_pads += 1
                        elif "audio" in pad.get_caps().to_string():
                            apad_added = self._handle_audio_pad_added(uridecode_elem, pad, audio_pads)
                            if apad_added:
                                audio_pads += 1

                    # remove timeout id - error after this is error in start
                    if self._timeoutid:
                        gobject.source_remove(self._timeoutid)
                        self._timeoutid = None

                    # unblocking all pads again (no matter what type)
                    for pad in uridecode_elem.pads():
                        pad.set_blocked_async(False, self._cb_unblocked)

                    if (audio_pads + video_pads) > 0:
                        self.start()
                        self.emit("pass-setup")
                    else:
                        # Send eos - that completes pass and then next pass can be started
                        self.pipe.post_message(gst.message_new_eos(self.pipe))

                except Exception as e:
                    _log.debug(e.message)
                    for pad in uridecode_elem.pads():
                        pad.set_blocked_async(False, self._cb_unblocked)
                    self.emit("error", e.message, 0)

        elif t == gst.MESSAGE_ASYNC_DONE:
            _log.debug("ASYNC_DONE msg received!")
        
        self.emit("message", bus, message)
    
    def _cb_unblocked(self, *args):
        _log.debug(args)

    def _cb_no_app_message_timeout(self):
        self.emit("error", "Pipeline Hanged. No application Message Received", 0)

    def start(self, reset_timer=True):
        """
            Start the pipeline!
        """
        self.state = gst.STATE_PLAYING
        if reset_timer:
            self.start_time = time.time()
    
    def pause(self):
        """
            Pause the pipeline!
        """
        self.state = gst.STATE_PAUSED

    def stop(self):
        """
            Stop the pipeline!
        """
        self.state = gst.STATE_NULL

    def get_state(self):
        """
            Return the gstreamer state of the pipeline.
            
            @rtype: int
            @return: The state of the current pipeline.
        """
        if self.pipe:
            return self.pipe.get_state()[1]
        else:
            return None
    
    def set_state(self, state):
        """
            Set the gstreamer state of the pipeline.
            
            @type state: int
            @param state: The state to set, e.g. gst.STATE_PLAYING
        """
        if self.pipe:
            self.pipe.set_state(state)
    
    state = property(get_state, set_state)
    
    def get_status(self):
        """
            Get information about the status of the encoder, such as the
            percent completed and nicely formatted time remaining.
            
            Examples
            
             - 0.14, "00:15" => 14% complete, 15 seconds remaining
             - 0.0, "Uknown" => 0% complete, uknown time remaining
            
            Raises EncoderStatusException on errors.
            
            @rtype: tuple
            @return: A tuple of percent, time_rem
        """
        start, stop = self.options.start_time, self.options.stop_time
        duration = max(self.info.videolength, self.info.audiolength)
        
        duration = self.output_duration * gst.SECOND
        if not duration or duration < 0:
            return 0.0, _("Unknown")
        
        try:
            pos, format = self.pipe.query_position(gst.FORMAT_TIME)
        except gst.QueryError:
            raise TranscoderStatusException(_("Can't query position!"))
        except AttributeError:
            raise TranscoderStatusException(_("No pipeline to query!"))
        
        pos = pos - self._start_ns
        percent = pos / float(duration)
        if percent <= 0.0:
            return 0.0, _("Unknown")
        
        if self._percent_cached == percent and time.time() - self._percent_cached_time > 5:
            self.pipe.post_message(gst.message_new_eos(self.pipe))
        
        if self._percent_cached != percent:
            self._percent_cached = percent
            self._percent_cached_time = time.time()
        
        total = 1.0 / percent * (time.time() - self.start_time)
        rem = total - (time.time() - self.start_time)
        min = rem / 60
        sec = rem % 60
        
        try:
            time_rem = _("%(min)d:%(sec)02d") % {
                "min": min,
                "sec": sec,
            }
        except TypeError:
            raise TranscoderStatusException(_("Problem calculating time " \
                                              "remaining!"))
        
        return percent, time_rem
    
    status = property(get_status)
    
