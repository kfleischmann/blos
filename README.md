
Install & Build
=============

```
git clone
cd blos/
sudo apt-get install jshon
mvn clean package
```

Environement variables needed
```
BLOS_PATH=/path/to/blos
FLINK_PATH=/path/to/flink
```


This library is built on  flink.apache.org. 

Used libraries

http://matplotlib.org/

How to generate and visualize datasets
=============
Samples 10.000 datapoints from a polynomial function within the range from -1 to 1 and visualize the output.
OutputFormat: CSV
Function: f(x) = 1*x^1 + 2*X^2 + ... {factor}:{exp} (more are possible)
```
 blos generators poly --sigma 0.01 -f 1:1,2:2 --range="-1:1" --count 10000 | blos visualize scatter2d
 blos generators poly --sigma 0.05 -f "100:0,0.5:1" --range="-1:1" --count 10000 | blos visualize scatter2d
 ```
Read data do regression and visualize data and show result. Please keep in your mind, that `regression linear` only allows the regression on linear m*x+c datasets. More regression may be supported in the future.

```
cat data | blos regression linear | blos visualize curve2d
cat data | blos regression poly | blos visualize curve2d
```

Linear-regression with visualization
```
blos generators poly --sigma 0.1 -f 1:1 --range="-1:1" --count 1000 | blos regression linear | blos visualize curve2d
blos generators poly --sigma 0.035 -f 0.2:1 --range="-1:1" --count 1000 | blos visualize scatter2d
```

Linear-Regression with Gradient-Decent using R 
```
cat dataset9|blos math gd
blos generators poly --sigma 0.01 -f 1:0,2:1 --range="-1:1" --count 4000| blos math gd
```

Linear-Regression with Gradient-Decent the sketches
```
cat dataset9 | blos run-examples SketchedLinearRegression -i stdin -n 10 -s 1 -s1 0.1:0.2 -s2 0.1:0.2 -s3 0.1:0.2 -s4 0.1:0.2 -s5 0.1:0.2 -s6 0.1:0.2 -v -d
```

Linear-Regression for real-model: y=0.6+0.1*x with 1Mio datapoints. Totoal Sketchsize 3mb
```
blos generators poly --sigma 0 -f 0.6:0,0.1:1 --range="-1:1" --count 1000000 -H no | blos run-examples SketchedLinearRegression -i stdin -n 50 -s 4 -s1 0.1:0.0001 -s2 0.1:0.0001 -s3 0.1:0.0001 -s4 0.1:0.0001 -s5 0.1:0.0001 -s6 0.1:0.0001 -v -d
```
Finally learned model: 0.5998466649164175 0.10477879077668788

KMeans dataset
```
blos examples run eu.blos.java.ml.clustering.KMeansDatasetGenerator \
	-points ${NUM_SAMPLES} \
	-k ${NUM_CENTROIDS} \
	-stddev ${STDDEV} \
	-range ${RANGE} \
	-output ${DATA_DIR}/dataset \
	-resolution ${RESOLUTION} \
	-seed ${SEED}
```


Examples
=============
SketchedLinearRegression

SketchedLogisticRegression

SketchedKMeans
```
$blos examples run eu.blos.java.ml.clustering.SketchedKMeans
LOG: Missing required options: i, k, s, n, p, H
usage: SketchedKMeans
 -a,--all-results                 show all model results
 -e,--enumeration <arg>           enumerate input space for reconstruction
 -H,--heavyhitters <arg>          HeavyHitters
 -h,--help                        shows valid arguments and options
 -i,--input                       set the input dataset to process
 -k,--centroids <arg>             set the number of centroids
 -n,--iterations <arg>            number of iterations
 -P,--print-sketch                only print sketch without running
                                  learning
 -p,--normalization-space <arg>   normalization-space
 -r,--init-randomly               only print sketch without running
                                  learning
 -s,--sketch <arg>                sketch size
 -v,--verbose                     verbose


$blos examples run eu.blos.java.ml.clustering.SketchedKMeans \
-i <datasets>/kmeans/dataset5_20k/points \
-k 5 \
-n 100 \
-p 4  \
-s 0.01:0.01 \
-H 100 
```

Sketch scatter-plot
===
```
# generate data
blos examples run eu.blos.java.ml.clustering.KMeansDatasetGenerator \
	-points 100000 \
	-k 3 \
	-stddev 0.07 \
	-range 1.0 \
	-output kmeans100k_3c/dataset \
	-resolution 3 \
	-seed 0
	
# sketch scatterplot
blos sketch scatterplot \
 --dataset-dir <datasetdir> \
 --sketch-p 3 \
 --sketch-delta "0.5 0.1 0.05 0.01" \
 --sketch-epsilon "0.1 0.01 0.001 0.0001"
```
