# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Correlations

# COMMAND ----------

# Get processed data, and take out TOTAL
crimes = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/datasets/processed-dataset.csv")
crimes = crimes.filter( crimes['state_name'] != "TOTAL")
# crimes.count()
crimes = crimes.withColumn("rape_final", crimes["rape_final"].cast('int'))
crimes.display()

# COMMAND ----------

## Unpivot the columns (columns -> rows), then pivot them for unique combinations
from pyspark.sql.functions import expr
from pyspark.sql.functions import concat, col, lit

def unpivot_main_table(df=None):
    unpivotExpr = "stack(10, 'population', population, 'violent_crime', violent_crime, 'homicide', homicide, 'robbery', robbery,'aggravated_assault',aggravated_assault,'property_crime',property_crime,'burglary',burglary,'larceny',larceny,'motor_vehicle_theft',motor_vehicle_theft,'rape_final',rape_final) as (crime,total)"
    unpivot_df = df.select("year","state_name", expr(unpivotExpr))
    unpivot_df = unpivot_df.withColumn('state_crime', concat(unpivot_df['state_name'],lit("_"), unpivot_df['crime']))
    unpivot_df = unpivot_df.select('year', 'total', 'state_crime')
    return unpivot_df

df = unpivot_main_table(df=crimes)
df.display()

# COMMAND ----------

### Pivot dataframe, rows -> columns
pivot_df = df.groupBy("year").pivot("state_crime").sum("total").orderBy("year")
pivot_df = pivot_df.drop('year')
pivot_df.display()

# COMMAND ----------

# Manual calculation of correlation matrix by looping
import pandas as pd
import pyspark.sql.functions as f
col_list = pivot_df.columns
col_list = col_list[0:10]

# Function to get correlation between two series
def get_correlation(df, col1, col2):
    corr_val = df.corr(col1, col2)
    return {'source': col1, 'destination':col2, 'correlation':corr_val}

# Loop to cycle through all combinations, and produce dataframe
corr_list = []
for i, v in enumerate(col_list):
    for j,k in enumerate(col_list):
        temp_val = get_correlation(pivot_df, col_list[i], col_list[j])
        corr_list.append(temp_val)
        
df_corr = pd.DataFrame(corr_list)
sparkdf = spark.createDataFrame(df_corr) # convert to Spark dataframe for unpivotting
corr_pivot = sparkdf.groupBy("source").pivot("destination").sum("correlation").orderBy("source")
corr_pivot.display()

# COMMAND ----------

data_location_root = "/FileStore/datasets/"
data_location = "/FileStore/datasets/correlation-matrix/"
corr_pivot.write.format("csv").option('header','true').mode("overwrite").save(data_location)

# Clear out additional files produced in directory to create single CSV file
files = dbutils.fs.ls(data_location)
csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
dbutils.fs.mv(csv_file, data_location.rstrip('/') + ".csv")
dbutils.fs.rm(data_location, recurse = True)
