Install & Build
=============
Environement variables needed
```
BLOS_PATH=/direct-me-to-blospath/
```


Scripts
=============
```
blos generators poly --help
usage: poly [-h] [-v] -s SIGMA -c COUNT -f FUNCTION -r RANGE [-H HEADER]

		create polynomial regression datasets
	

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         turns on verbosity
  -s SIGMA, --sigma SIGMA
                        variance
  -c COUNT, --count COUNT
                        number of samples
  -f FUNCTION, --function FUNCTION
                        factor1:exp1,..
  -r RANGE, --range RANGE
                        x1:x2
  -H HEADER, --header HEADER
                        TRUE or FALSE
```

Samples 10.000 datapoints from a polynomial function within the range from -1 to 1 and visualize the output.
OutputFormat: CSV
Function: f(x) = 1*x^1 + 1*X^2 +... (more are possible)
```
 blos generators poly --sigma 0.01 -f 1:1,2:1 --range="-1:1" --count 1000 | blos visualize vis2d
 ```


Examples
=============
