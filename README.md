Install & Build
=============
Environement variables needed
```
BLOS_PATH=/direct-me-to-blospath
FLINK_PATH=/direct-me-to-flinkpath
```

For scripts framework
```
sudo apt-get install jshon
mvn clean package
```

This library is built on flink.apache.org


Scripts
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

Merge datasets
```
{
blos stream add dataset1; blos generators poly --sigma 0.01 -f 1:1,1:2 --range="-1:1" --count 10000 ;
blos stream add dataset2; blos generators poly --sigma 0.01 -f 1:1,1:2 --range="-1:1" --count 10000 ;
}
| blos visualize scatter2d-stream
```

Linear-regression with visualization
```
blos generators poly --sigma 0.1 -f 1:1 --range="-1:1" --count 1000 | blos regression linear | blos visualize curve2d
blos generators poly --sigma 0.035 -f 0.2:1 --range="-1:1" --count 1000 | blos visualize scatter2d
```

Linear-Regression with Gradient-Decent
```
 cat dataset7|blos math gd
```


Examples
=============
linear regression
```
blos run-examples linear-regression-on-sketches --preprocessor --input-path /dataset --output-path hdfs://blos/linreg1
blos run-examples linear-regression-on-sketches --sketcher --input-path hdfs:///blos/dataset --output-path hdfs:///blos/linreg1
blos run-examples linear-regression-on-sketches --learner --input-path hdfs:///blos/dataset --output-path hdfs:///blos/linreg1 
cat lin-reg-model | blos visualize curve2d

# Run preprocessor sketcher and learner
blos run-examples linear-regression-on-sketches  -p -s -l --input-path hdfs:///blos/dataset --output-path hdfs://blos/linreg1
```

Generic Usage
=============
