Install & Build
=============
Environement variables needed
```
BLOS_PATH=/direct-me-to-blospath/
```


Scripts
=============


Samples 10.000 datapoints from a polynomial function within the range from -1 to 1 and visualize the output.
OutputFormat: CSV
Function: f(x) = 1*x^1 + 1*X^2 +... (more are possible)
```
 blos generators poly --sigma 0.01 -f 1:1,2:1 --range="-1:1" --count 10000 | blos visualize vis2d
 ```


Examples
=============
