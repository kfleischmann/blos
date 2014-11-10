#!/usr/bin/env python

import numpy as np
import matplotlib.pyplot as plt
from sklearn import datasets, linear_model
import sys

zip_with = lambda fn, la, lb: [fn(a, b) for (a, b) in zip(la, lb)]

if __name__=='__main__':
	if len(sys.argv) < 5:
		print "wrong arguments m*x+c"
		print "m c count sigma"
		sys.exit(0)

	sigma = float(sys.argv[4])
	samples = int(sys.argv[3])
	m = float(sys.argv[1])
	c = float(sys.argv[2])
	sep=","



	xx = []
	yy = []

	def linear_func(x):
		return m*x+c
	x=0
	counter=0
	for s in range(0, samples):
		y=round(linear_func(x) + np.random.uniform(-1,1)*sigma )
		#xx.append(x)
		#yy.append(y)
		x = x + np.random.uniform(0,sigma)
		print str(counter)+sep+str(x)+sep+str(y)

		counter=counter+1
	# scale it
	#xx_max = max(xx)
	#yy_max = max(yy)
	#xx = map( lambda x : x/ xx_max, xx )
	#yy = map( lambda y : y/ yy_max, yy )

	#scaled = zip_with(lambda x,y: [x,y], xx,yy)
	#for s in scaled:
	#	print str(s[0])+sep+str(s[1])