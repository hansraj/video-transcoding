Arista Transcoder 0.9.8
=======================
A simple preset-based transcoder for the GNOME Desktop and a small script for 
terminal-based transcoding. Settings are chosen based on output device and 
quality preset.

 * [Official website](http://www.transcoder.org/)

Features
--------
 * Presets for Android, iPod, computer, DVD player, PSP, and more
 * Live preview to see encoded quality
 * Automatically discover available DVD drives and media
 * Rip straight from DVD media easily
 * Automatically discover and record from Video4Linux devices
 * Support for H.264, WebM, FLV, Ogg, DivX and more
 * Batch processing of entire directories easily
 * Simple terminal client for scripting
 * Nautilus extension for right-click file conversion

Dependencies
------------
 * python >=2.4
 * python-cairo
 * python-gobject
 * python-gtk >=2.16
 * python-gconf
 * python-gstreamer
 * python-gudev or python-dbus with HAL
 * python-nautilus (if using Nautilus extension)
 * python-pynotify (optional)
 * python-rsvg (if using KDE)
 * python-simplejson (if using python 2.5 or older)
 * gstreamer-ffmpeg
 * gstreamer-plugins-base
 * gstreamer-plugins-good
 * gstreamer-plugins-bad
 * gstreamer-plugins-ugly
 * python-webkit (for in-app documentation)
 * lsdvd (to read DVD title information)

Debian users may need to install these additional dependencies:

 * gstreamer0.10-lame
 * gstreamer0.10-plugins-really-bad

Fedora users first need to enable RPM Fusion repo because they won't have dependencies for Arista (http://rpmfusion.org/). Then install these additional dependencies:

 * gnome-python2-rsvg
 * nautilus-python (if using Nautilus extension)
 * gstreamer-plugins-bad-nonfree

Installation
------------
Installation uses python distutils. After extracting the archive, run:

    python setup.py install

If you are using Ubuntu 9.04 (Jaunty) or later, make sure to install with:

    python setup.py install --install-layout=deb

Don't forget to use sudo if necessary. This will install the arista python 
module to your python site-packages or dist-packages path, install the arista 
programs into sys.prefix/bin, instal the nautilus extensions into 
sys.prefix/lib/nautilus/extensions-2.0/python and install all data files into 
sys.prefix/share/arista.

You can also choose to install the Nautilus extension per-user by placing it 
into the user's home directory under ~/.nautilus/python-extensions. Note
that you must restart Nautilus for such changes to take effect!

Usage
-----
There are two clients available, a graphical client using GTK+ and a terminal 
client. The graphical client is failry self-explanatory and can be launched 
with:

    arista-gtk

To use the terminal client please see:

    arista-transcode --help

An example of using the terminal client:

    arista-transcode --device=apple --preset="iPhone / iPod Touch" test.avi

Other useful terminal options:

    arista-transcode --info
    arista-transcode --info apple iPad

There is also a Nautilus extension installed by default. You can right-click on
any media file and select "Convert for device..." in the menu. This menu
contains all your installed presets and will launch Arista to convert the
selected file or files.

Generating a Test File
----------------------
Sometimes it may be useful to generate a test file:

    gst-launch-0.10 videotestsrc num-buffers=500 ! x264enc ! qtmux ! filesink location=test.mp4

Trying the Latest Version
-------------------------
You can try out the latest development version of Arista by grabbing and running the code from git. This lets you test issues you may have against the latest work of the developers as well as try out new features. Try running the following in a terminal:

    git clone git://github.com/danielgtaylor/arista.git
    cd arista
    ./arista-gtk

Contributing
------------
All active development has moved to GitHub.com. Code is managed through git and
new bugs should be opened in the GitHub tracker. Translations are still managed
on Launchpad using a bazaar tracking branch of this git repository. The 
GitHub page is here:

 * [Github project page](http://github.com/danielgtaylor/arista)
 * [Translations page](https://translations.launchpad.net/arista)

You can grab a copy of the source code for Arista via:

    git clone git://github.com/danielgtaylor/arista.git

Feel free to fork on GitHub and propose updates to the main branch. You can
keep your branch up to date via `git pull`

The public website of this project is also open source, and can be found here:

 * [Website project page](http://github.com/danielgtaylor/arista-website)

Feel free to fork it and contribute as well.

