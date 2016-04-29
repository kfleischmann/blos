#!/usr/bin/env python
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import sys

values = []
dpi = 100
outputFile = sys.argv[1]

for line in sys.stdin:
	values.append( int(line) )

# the histogram of the data
n, bins, patches = plt.hist(values, 50, facecolor='green', alpha=0.75)


plt.ylabel('# counts',fontsize=14 )
plt.xlabel('# estimate error',fontsize=14 )

#plt.show()

plt.savefig(outputFile, dpi=dpi, bbox_inches='tight' )
