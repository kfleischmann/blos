#!/usr/bin/env python

################################################################################
#
# Authors:       Fleischmann, Kay (fleischmann.kay@googlemail.com)
# Documentation: -
#
################################### LICENSE ####################################
#
# Copyright (C) 2014 Fleischmann, Kay
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
################################ DOCUMENTATION #################################
#
# see output (-h)
#
#
#
################################################################################
import matplotlib.pyplot as plt
from numpy.random import rand
from PIL import Image
from os.path import basename


def run(**kwargs):
	format=kwargs['output_size'].split("x")
	max_d = 0
	max_w = 0
	max_count = 0

	# read statistics
	with open(kwargs['sketch']) as f:
		for line in f:
			fields=line.strip().split(',')
			w=int(fields[0])
			d=int(fields[1])
			count=int(fields[2])

			max_w = max(w,max_w)
			max_d = max(d,max_d)
			max_count = max(count,max_count)

	white = (255,255,255)
	img = Image.new("RGB", [max_w+1,max_d+1], white)

	print "Max w:", max_w
	print "Max d:", max_d
	print "Max count:", max_count

	# read statistics
	with open(kwargs['sketch']) as f:
		for line in f:
			fields=line.strip().split(',')
			w=int(fields[0])
			d=int(fields[1])
			count=int(fields[2])

			s = int( float(count)/float(max_count)*255.0)
			color = ( s ,s ,s )
			img.putpixel((w, d), color)

	img2=img.resize((int(format[0]),int(format[1])))
	output=kwargs['output_dir']+basename(kwargs['sketch'])+"."+kwargs['output_format']
	img2.save(output)

	if bool(kwargs['display']):
		img2.show()


if __name__=='__main__':
	# handle command line arguments
	# http://docs.python.org/2/library/argparse.html
	import argparse
	from argparse import RawTextHelpFormatter

	argparser = argparse.ArgumentParser(description="""
		visualize the sketch
	""", formatter_class=RawTextHelpFormatter)


	# specify arguments
	argparser.add_argument("-v", "--verbose", action="store_true", help="turns on verbosity")
	argparser.add_argument("-s", "--sketch", action="store", required=True, help="sketch file")
	argparser.add_argument("-o", "--output_dir", action="store", required=True, help="Path to output directory")
	argparser.add_argument("-S", "--output_size", action="store", required=False, help="output image format", default="1000x100")
	argparser.add_argument("-f", "--output_format", action="store", required=False, help="output image format", default="jpg")
	argparser.add_argument("-d", "--display", action="store_true", required=False, help="display image" )

	# validate arguments
	args = argparser.parse_args()

	try:
		# execute main script and delegate command line options
		run(**vars(args))

	except Exception as e:
		print "AN ERROR OCCOURED:", e

		if( bool(vars(args)['verbose']) ):
			import traceback, sys
			traceback.print_exc(file=sys.stderr)