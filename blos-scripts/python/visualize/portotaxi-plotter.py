#!/usr/bin/python

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
# the script produce some nice plots for the portotaxi example
#
#
# results: index
# 0: ${W},
# 1: ${D},
# 2: ${SKETCH_SIZE},
# 3: ${TOTAL_ERRORS},
# 4: ${PRECISION},
# 5: ${RECALL},
# 6: ${REAL_ERROR_PER_QUERY},
# 7: ${ESTIMATED_ERROR_PER_QUERY},
# 8: ${DISTINCT_KEYS}"
#
################################################################################
import sys
import matplotlib.pyplot as plt
import itertools

def extactData(lines, delimiter, index):
	data = []
	for l in lines:
		data.append(l.split(delimiter)[index])
	return data

def run(**kwargs):
	fig = plt.figure()
	ax1 = fig.add_subplot(111)

	inputFile 		= kwargs["input"]
	outputFile 		= kwargs["output"]
	delimiter 		= kwargs["delimiter"]
	dpi  			= 100
	groupindex		= int(kwargs["groupby"])
	index_valx		= int(kwargs["xval"])
	index_valy		= int(kwargs["yval"])
	labelx			= kwargs["xlabel"].replace("-"," ")
	labely			= kwargs["ylabel"].replace("-"," ")


	if kwargs["dpi"]:
		dpi = int(kwargs["dpi"])

	with open(inputFile) as f:
		lines = f.readlines()

		dlist = extactData(lines, delimiter, groupindex)	# default differntiate between d
		xlist = extactData(lines, delimiter, index_valx )
		ylist = extactData(lines, delimiter, index_valy )

		values = zip(dlist,xlist,ylist)
		# sort by first value, to be grouped
		values.sort(key= lambda x: x[0], reverse=True)

		# plot for each d

		# sort sketch counts from left top top right bottom
		for d,g in itertools.groupby(values, lambda x: x[0]):
			measurements = list(g)
			x =  map( lambda x: x[1], measurements )
			y =  map( lambda x: x[2], measurements )

			ax1.plot(x, y, '-s',  label='d='+d )

	ax1.legend(framealpha=0.5)
	plt.ylabel(labely, fontsize=14 )
	plt.xlabel(labelx, fontsize=14 )

	plt.savefig(outputFile, dpi=dpi, bbox_inches='tight' )

	print "\nPlotted file: %s" % outputFile

	sys.exit(0)

if __name__ == "__main__":
	# handle command line arguments
	# http://docs.python.org/2/library/argparse.html
	import argparse
	from argparse import RawTextHelpFormatter

	argparser = argparse.ArgumentParser(description="""
	blos kmeans-plotter.
	""", formatter_class=RawTextHelpFormatter)

	# specify arguments
	argparser.add_argument("-v", "--verbose",   	action="store_true", 	help="turns on verbosity")
	argparser.add_argument("-H", "--hideaxis",  	action="store_true", 	help="hide axis text")
	argparser.add_argument("-i", "--input", 		action="store", 		required=True, help="input file")
	argparser.add_argument("-D", "--delimiter", 	action="store", 		required=True, help="result delimiter" )
	argparser.add_argument("-o", "--output", 		action="store", 		required=True, help="output file")
	argparser.add_argument("-d", "--dpi", 			action="store", 		required=False, help="dpi of the output file")

	argparser.add_argument("-X", "--xlabel", 		action="store", 		required=True, help="xlabel")
	argparser.add_argument("-Y", "--ylabel", 		action="store", 		required=True, help="ylabel")
	argparser.add_argument("-G", "--groupby", 		action="store", 		required=True, help="groupby")

	argparser.add_argument("-x", "--xval", 			action="store", 		required=True, help="value for x")
	argparser.add_argument("-y", "--yval", 			action="store", 		required=True, help="value for y")


	# validate arguments
	args = argparser.parse_args()


	run(**vars(args))
