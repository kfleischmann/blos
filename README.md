Install & Build
=============
Environement variables needed
```
BLOS_PATH=/direct-me-to-blospath/
```


Scripts
=============
Samples 200 datapoints from a polynomial function within the range -10 and 10 and visualize the output.
OutputFormat: CSV
```
blos generators poly -s 100 -f 1:1,2:2 --range="-10:10" -c 200 | blos visualize vis2d
```


Examples
=============
