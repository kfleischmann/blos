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
def run(**kwargs):
	print "main"

	print kwargs('verbose')

if __name__=='__main__':
	# handle command line arguments
	# http://docs.python.org/2/library/argparse.html
	import argparse
	from argparse import RawTextHelpFormatter

	argparser = argparse.ArgumentParser(description="""
		Organize the blos experiments. You can add experiments.
	""", formatter_class=RawTextHelpFormatter)


	# specify arguments
	argparser.add_argument("-v", "--verbose", action="store_true", help="turns on verbosity")
	argparser.add_argument("-s", "--sketch-dir", action="store", required=True, help="Path to sketch directory")
	argparser.add_argument("-o", "--output_dir", action="store", required=True, help="Path to output directory")

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