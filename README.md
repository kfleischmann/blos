Install & Build
=============
Environement variables needed
```
BLOS_PATH=/direct-me-to-blospath/
```


Scripts
=============
Samples 10.000 datapoints from a polynomial function within the range -100 and 100 and visualize the output.
OutputFormat: CSV
Function: f(x) = 3*x^1 + 1*X^2 +... (more are possible)
```
 blos generators poly --sigma 1000 -f 1:3,2:1 --range="-100:100" --count 10000 | blos visualize vis2d
 ```


Examples
=============
