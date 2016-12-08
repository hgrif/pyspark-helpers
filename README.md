# PySpark helpers

Collection of PySpark helpers.


## UDAF

`udaf()` applies a custom groupby function to one or more PySpark DataFrames.
This is an (ugly) way to convert your pandas functions into a parallelized Spark action.

PySpark currently only supports user defined aggregated functions (UDAFs) if they're composed of PySpark functions.
This works great if your algorithm can be expressed in those functions but it limits you if you need to do more advanced, unsupported stuff.
Also, work is often prototyped in pandas on smallish data (e.g. data for 1 customer) and has to be parallelized on biggish data (e.g. data for 1 million customers).

Say you get a slice of data from your big PySpark DataFrame:
