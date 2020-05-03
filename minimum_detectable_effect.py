## ---- IMPORT YOUR PYTHON PACKAGES -----

import pandas as pd
import numpy as n
import matplotlib as mpl
from pyhive import presto



## ----- CONNECT TO YOUR QUERY ENGINE ------

## this framework hooks directly into Presto to directly query a table - but of course you can also import your data from a CSV with pd.read_csv, etc.
def get_data(query):
    engine = presto.connect('insert_your__presto_url', catalog='insert_your_catalog', schema='insert_schema_you_are_querying')
    df = pd.read_sql(query, engine)
    return df



## ------ BRING THE HISTORICAL DATA YOU ARE COMPARING TO BASELINE -----

##insert your query with historical data (by day) here:
historical_daily_data = """
select
* from example_historical_table
) a
"""


## ----- BRING THE BASELINE DATA IN AGGREGATE 
##insert your  query to get baseline data (not by day) here:
baseline_data = """
select
*
from example_baseline_data_table
) a
"""


df = get_data(historical_daily_data)

b = get_data(baseline_data)


## ------ CALCULATE MEAN, VARIANCE, SAMPLE SIZE -----

## Example Rate 1
a = n.array(df.example_rate1, dtype=n.float64)

## Example Rate 2
##a = n.array(df.example_rate2, dtype=n.float64)
variance = n.var(a)
mean = n.mean(a)

## input number of days you plan to run the test
test_days = (14) 

## input daily sample size across all slices
daily_total_sample_size = b.avg_daily_sample_size

## input the percentage split of the test, e.g. on a 50/50 test you would input 0.5
control_test_split = (0.33)

##automatically calculated control sample size
control_sample_size = (((daily_total_sample_size) * (control_test_split)) * (test_days))


## ----- CALCULATE MINIMUM DETECTABLE EFFECT ------ 

minimum_detectable_effect = n.absolute (
n.absolute(
((2.8 / (n.sqrt(n.sum(control_sample_size[0])/2)) * (n.sqrt(variance))) - mean)
)
/
(mean - 1)
)

mde_result = ("Min. Detectable Effect = " + str(n.round((minimum_detectable_effect * 100),decimals=2)) + "%")
variance_result = ("Variance = " + str(n.round(variance,decimals=10)))
mean_result = ("Mean = " + str(n.round((mean * 100),decimals=2)) + "%")
sample_size_result = ("Control Sample Size = " + str(n.round(control_sample_size[0],decimals=0)))

print(variance_result)
print(mean_result)
print(sample_size_result)
print(mde_result)


## ---- DETERMINE NUMBER OF DAYS / WEEKS NEEDED TO RUN TEST -----

## baseline rate you are going off of, using predetermined historical window.
baseline_rate = b.example_rate1[0]

expected_improvement = minimum_detectable_effect

## number of variations of the test running at the same time. In this example it is a 33/33/33, so two variations of test and one control.
num_variations = 2 

avg_daily_sample_size = daily_total_sample_size[0]


## number of users in your sample needed to reach 80% confidence 
num_sample_size_needed_80 = ((num_variations) * (16 * n.power(
    n.sqrt(baseline_rate * (1- baseline_rate))
    / 
    (baseline_rate * expected_improvement),2)
))

## number of users in your sample needed to reach 95% significance
num_sample_size_needed_95 = ((num_variations) * (26 * n.power(
    n.sqrt(baseline_rate * (1- baseline_rate))
    / 
    (baseline_rate * expected_improvement),2)
))

num_days_needed_95 = round((num_sample_size_needed_95 / avg_daily_sample_size), 2)
num_days_needed_80 = round((num_sample_size_needed_80 / avg_daily_sample_size), 2)

## ---- PRINT FINAL RESULTS ----- 
print("Baseline rate: " + str(round((baseline_rate * 100),2))+ "%")
print("Number of days needed to reach 95% Significance: " + str(num_days_needed_95))
print("Number of days needed to reach 80% Significance: " + str(num_days_needed_80))

