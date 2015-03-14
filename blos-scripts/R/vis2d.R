#!/usr/bin/env Rscript

data <- read.table( file("stdin"), sep=",", header=TRUE)
plot(data$x, data$y, type='p', cex=0.5, col="red")

