Install & Build
=============
Environement variables needed
```
BLOS_PATH=/direct-me-to-blospath/
```

For scripts framework
```
sudo apt-get install jshon
```

Scripts
=============


Samples 10.000 datapoints from a polynomial function within the range from -1 to 1 and visualize the output.
OutputFormat: CSV
Function: f(x) = 1*x^1 + 1*X^2 +... (more are possible)
```
 blos generators poly --sigma 0.01 -f 1:1,2:1 --range="-1:1" --count 10000 | blos visualize scatter2d
 blos generators poly --sigma 0.05 -f "0:100,1:0.5" --range="-1:1" --count 10000 | blos visualize scatter2d
 ```

Read data do regression and visualize data and show result
```
cat data | blos regression linear | blos visualize curve2d
cat data | blos regression poly | blos visualize curve2d
```

Merge datasets
```
{
blos stream add dataset1; blos generators poly --sigma 0.01 -f 1:1,2:1 --range="-1:1" --count 10000 ;
blos stream add dataset2; blos generators poly --sigma 0.01 -f 1:1,2:1 --range="-1:1" --count 10000 ;
}
| blos visualize scatter2d-stream
```

Linear-regression with visualization
```
blos generators poly --sigma 0.1 -f 1:1 --range="-1:1" --count 1000 | blos regression linear | blos visualize curve2d
blos generators poly --sigma 0.035 -f 1:0.2 --range="-1:1" --count 1000 | blos visualize scatter2d
```

Usage
=============
Regression with Sketching techniques
```
cat data | blos ml-sketcher linear-regression --input stdin --output stdout | blos visualize curve2d
blos ml-sketcher linear-regression --input hdfs:///dataset --output hdfs:///results
```

Workflow
=============
```
dataset -> linreg.prepare -> preprocessed-data
preprocessed-data -> linreg.sketcher -> sketched-data
sketched-data -> linreg.learner -> learn parameters

```


Examples
=============
