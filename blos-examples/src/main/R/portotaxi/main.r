#!/usr/bin/env Rscript
#
# source http://archive.ics.uci.edu/ml/machine-learning-databases/00339/

args = commandArgs(trailingOnly=TRUE)

# lets say we are in the root path
path <- paste( Sys.getenv("BLOS_PATH"), "/", sep="" )

traindataset_downloadlink <- "http://archive.ics.uci.edu/ml/machine-learning-databases/00339/train.csv.zip"
testdataset_downloadlink <- "http://archive.ics.uci.edu/ml/machine-learning-databases/00339/Porto_taxi_data_test_partial_trajectories.csv"

local_traindataset <- paste(path,"datasets/portotaxi/train.csv.zip", sep="")
local_testdataset <-  paste(path,"datasets/portotaxi/test.csv", sep="")


# download test dataset
#if(!file.exists(local_testdataset)){
#	print("test dataset not exists. download taxi test dataset")
#	dir.create(dirname(local_testdataset), recursive=TRUE)
#	download.file(testdataset_downloadlink, local_testdataset)
#} else {
#	print("test dataset exists")
#}
#
#
## download train dataset
#if(!file.exists(local_traindataset)){
#	print("train dataset not exists. download taxi train dataset")
#	dir.create(dirname(local_traindataset), recursive=TRUE)
#	download.file(traindataset_downloadlink, local_traindataset)
#} else {
#	print("train dataset exists")
#}


# load libraries and functiony
source(paste(path,"blos-examples/src/main/R/portotaxi/functions.r", sep=""));

inputfile <- args[1]
# e.g. "datasets/portotaxi/taxi2_1400.tsv"

process.data(inputfile)