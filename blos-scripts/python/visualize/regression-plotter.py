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
#
# see output (-h)
#
#
#
################################################################################
import sys
import matplotlib.pyplot as plt
import csv
import os
import math
import numpy as np

def apply_func(type, model, t):
	if type=='linear':
		return model[0]+t*model[1]
	if type=='logistic':
		return 1.0 / (1 + np.exp(-(model[0]+t*model[1])))
	return None

def run(**kwargs):
	inputFile 		= kwargs["input"]
	outputFile 		= kwargs["output"]
	functions	 	= kwargs["functions"]
	functype	 	= kwargs["functype"]

	show = 'dataset' #kwargs["show"].lower()

	fig = plt.figure()
	ax1 = fig.add_subplot(111)


	cs = []
	xs = []
	ys = []

	dpi  = 100
	minX = None
	maxX = None
	minY = None
	maxY = None
	index = "0,1"
	delimiter = ' '

	if kwargs['delimiter']:
		delimiter=kwargs['delimiter'];

	if kwargs['index']:
		index=kwargs['index'];

	if kwargs["dpi"]:
		dpi = int(kwargs["dpi"])

	if show == "dataset":
		fig = plt.figure()
		ax1 = fig.add_subplot(111)

		indicies = map(lambda x:int(x), index.split(","))

		with open(inputFile, 'rb') as file:
			for line in file:
				# parse data
				csvData = line.strip().split(delimiter)

				x = float(csvData[indicies[0]])
				y = float(csvData[indicies[1]])

				if minX == None or minX > x:
					minX = x
				if maxX == None or maxX < x:
					maxX = x
				if minY == None or minY > y:
					minY = y
				if maxY == None or maxY < y:
					maxY = y

				xs.append(x)
				ys.append(y)

			# plot data
			ax1.scatter(xs, ys, s=25, c="#5b5b5b", edgecolors='None', alpha=0.2)

	t = np.arange(minX, maxX, 0.2)

	ax1.set_xlim([minX,maxX])
	ax1.set_ylim([minY,maxY])


	if functions:
		for i,f in enumerate(functions.split(",")):
			model = map( lambda x:float(x), f.split(":"))
			ax1.plot(t, apply_func(functype, model, t), linewidth=3.0)

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
	argparser.add_argument("-o", "--output", 		action="store", 		required=True, help="output file")
	argparser.add_argument("-d", "--dpi", 			action="store", 		required=False, help="dpi of the output file")
	argparser.add_argument("-g", "--grid", 			action="store_true", 	required=False, help="shows a grid")
	argparser.add_argument("-R", "--gres", 			action="store", 		required=False, help="grid resolution min,max,space")
	argparser.add_argument("-F", "--functions", 	action="store", 		required=False, help="func1 y=a+bx, a:b,a1:b1,...")
	argparser.add_argument("-T", "--functype",	 	action="store", 		required=False, help="linear|logistic")
	argparser.add_argument("-D", "--delimiter", 	action="store", 		required=False, help="set delimiter")
	argparser.add_argument("-I", "--index", 		action="store", 		required=True, 	help="set row fieldindex, x,y")
	#argparser.add_argument("-s", "--show",			action="store", 		required=False, help="result|dataset")



	# validate arguments
	args = argparser.parse_args()


	run(**vars(args))
