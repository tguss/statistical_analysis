import snowflake.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy.dialects import registry
registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from datetime import datetime as dt


snowflake = snowflake.connector.connect(
    user='xxxx'
    password='xxxxxx',
    account='xxxxxx',
    warehouse='xxxxxx'
    )

def get_data(query):
    engine = snowflake
    df = pd.read_sql(query, engine)
    return df

##determine bins for independent variables (x-axis of plot) and dependent variable (y-axis). in this example we pull from a Snowflake DB.
i = ("""
select
x -- independent variable
y -- dependent variable
from example
""")

df = pd.DataFrame(get_data(i))

x = np.array(df["X"])
y = np.array(df["Y"])
plt.plot(x, y, 'o')

m, b = np.polyfit(x, y, 1)

## insert a best fit line into the plot based on the slope of the pearson correlation line
best_fit = (m*x + b)

##insert columns into DataFrame to account for values + best fit plots
df.insert(2,column="Y_BEST_FIT", value = pd.DataFrame(best_fit))
df.rename(columns={"X" : "VALUE"}, inplace=True)
df.insert(0,column="MEASURE_NAME", value = "X")
df.insert(3,column="DATE_STAMP", value = dt.now().date())

engine = create_engine(URL(
    account = 'xxxxxxx',
    user = 'xxxxxxx',
    password = 'xxxxxxx',
    database = 'xxxxxxx',
    schema = 'xxxxxxx',
    warehouse = 'xxxxxxxxx',
    role='xxxxxxxxxx',
))

connection = engine.connect()
 
 ## (optional) write the dataframe back to a table in Snowflake for analysis.
df.to_sql('best_fit_example_table', con=engine, if_exists='append', index=False)

connection.close()
engine.dispose()





