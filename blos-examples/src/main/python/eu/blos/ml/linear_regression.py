#!/usr/bin/env python


from sklearn import datasets, linear_model

import numpy as np
import matplotlib.pyplot as plt

class Distribution(object):

	def __init__(self, x, y ):
		self.x = x
		self.y = y

		#sort the pdf by magnitude
		if self.sort:
			self.sortindex = np.argsort(self.pdf, axis=None)
			self.pdf = self.pdf[self.sortindex]
		#construct the cumulative distribution function
		self.cdf = np.cumsum(self.pdf)

	@property
	def ndim(self):
		return len(self.shape)
	@property
	def sum(self):
		"""cached sum of all pdf values; the pdf need not sum to one, and is imlpicitly normalized"""
		return self.cdf[-1]
	def __call__(self, N):
		"""draw """
		#pick numbers which are uniformly random over the cumulative distribution function
		choice = np.random.uniform(high = self.sum, size = N)
		#find the indices corresponding to this point on the CDF
		index = np.searchsorted(self.cdf, choice)
		#if necessary, map the indices back to their original ordering
		if self.sort:
			index = self.sortindex[index]
		#map back to multi-dimensional indexing
		index = np.unravel_index(index, self.shape)
		index = np.vstack(index)
		#is this a discrete or piecewise continuous distribution?
		if self.interpolation:
			index = index + np.random.uniform(size=index.shape)
		return self.transform(index)


if __name__=='__main__':
	x = np.linspace(-100, 100, 512)

#if __name__ == "__main__":
	#XY=func(100)
	#X=map(lambda x:x[0],XY)
	#Y=map(lambda x:x[1],XY)

	# Create linear regression object
	#regr = linear_model.LinearRegression()

	#plt.scatter(X,Y)
	#plt.show()

	# Load the diabetes dataset

	'''
	diabetes = datasets.load_diabetes()

	print len( diabetes.data )


	# Use only one feature
	diabetes_X = diabetes.data[:, np.newaxis]
	diabetes_X_temp = diabetes_X[:, :, 2]


	# Split the data into training/testing sets
	diabetes_X_train = diabetes_X_temp[:-20]
	diabetes_X_test = diabetes_X_temp[-20:]

	# Split the targets into training/testing sets
	diabetes_y_train = diabetes.target[:-20]
	diabetes_y_test = diabetes.target[-20:]

	# Create linear regression object
	regr = linear_model.LinearRegression()

	# Train the model using the training sets
	regr.fit(diabetes_X_train, diabetes_y_train)


	# The coefficients
	print('Coefficients: \n', regr.coef_)
	# The mean square error
	print("Residual sum of squares: %.2f"
		  % np.mean((regr.predict(diabetes_X_test) - diabetes_y_test) ** 2))
	# Explained variance score: 1 is perfect prediction
	print('Variance score: %.2f' % regr.score(diabetes_X_test, diabetes_y_test))

	# Plot outputs
	plt.scatter(diabetes_X_test, diabetes_y_test,  color='black')
	plt.scatter(diabetes_X_train, diabetes_y_train,  color='red')


	plt.plot(diabetes_X_test, regr.predict(diabetes_X_test), color='blue',
			 linewidth=3)

	plt.xticks(())
	plt.yticks(())

	plt.show()
	'''
