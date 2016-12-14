# PySpark helpers

Collection of PySpark helpers.


## UDAF

`udaf()` applies a custom groupby function to one or more PySpark DataFrames.
This is a (quick and dirty) way to convert your pandas functions into a parallelized Spark action.

PySpark currently only supports user defined aggregated functions (UDAFs) if they're composed of PySpark functions.
This works great if your algorithm can be expressed in those functions but it limits you if you need to do more advanced, unsupported stuff.
Also, work is often prototyped in pandas on smallish data (e.g. data for 1 user) and has to be parallelized on biggish data (e.g. data for 1 million users).

Say you get a slice of data from your big PySpark DataFrame, save it locally and starting prototyping a function with Pandas to comput profit:

```{python}
import pandas as pd


money = pd.DataFrame({'user': ['a', 'a', 'a', 'b'],
                      'money': [1, 1, 1, 4]})
multiplier = pd.DataFrame({'user': ['a', 'b'], 
                           'multiplier': [1, -1]})
                           
def profit(user_money, user_multiplier):
    return user_money.sum() * user_multiplier['multiplier'].values
    
p = []
for user in ['a', 'b']:
    p.append(profit(money[money['user'] == user]], 
                    multiplier[multiplier['users'] == user]))
```

Profit!

Now you want to apply this function on your original Spark DataFrames, `money_sdf` and `multiplier_sdf` for thousands of users, but you have no time to write a proper Spark function.
Just plug everything in `udaf()`:

```{python}
import pysparkhelpers


p_sdf = pysparkhelpers.udaf('user', profit, money_sdf, multiplier_sdf)
```

More profit!
