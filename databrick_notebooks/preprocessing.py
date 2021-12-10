# Databricks notebook source
# MAGIC %md
# MAGIC ### Clean the raw data, publish results to DBFS

# COMMAND ----------

# Load the main CSV file, remove columns that are not needed e.g. "caveats", and replace NULLs in state names, rape columns
# Total rows that should be outputted after cleaning: 2,184 (42 years * 52 states + TOTAL entries = 2,184)
# crimes = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/datasets/estimated_crimes_1979_2020.csv")
crimes = spark.read.format("csv").option("header", "true").option("inferSchema",
        "true").load("file:/Workspace/Repos/<User>/exploring-crime-data/datasets/estimated_crimes_1979_2020.csv")
crimes = crimes.fillna(value=0,subset=["rape_legacy", "rape_revised"])
crimes = crimes.fillna(value="TOTAL",subset=["state_abbr", "state_name"])
crimes = crimes.drop('caveats')
crimes = crimes.dropna()
crimes.count()

# COMMAND ----------

crimes.display()

# COMMAND ----------

# Source: https://ucr.fbi.gov/crime-in-the-u.s/2016/crime-in-the-u.s.-2016/resource-pages/rape-addendum
# Definitional change of 'rape' in 2013, with adaptations by various states on different timelines
# Two columns identify rape pre and post change, i.e. rape_legacy, rape_revised
# Given 'rape_revised' is always higher, using this ratio to markup prior years, based on average
from pyspark.sql import functions as f
from pyspark.sql.functions import col

# Filter for the years where this is filled by state
rape_df = crimes['state_abbr','rape_legacy', 'rape_revised'].filter( crimes['rape_legacy'].isNotNull() & crimes['rape_revised'].isNotNull())

# Aggregate by state for periods where both columns are filled
rape_df = rape_df.groupBy("state_abbr").agg(
    f.sum("rape_legacy").alias("RL"), 
    f.sum("rape_revised").alias("RR")
)

# Create average markup
rape_df = rape_df.withColumn('DEF_CHANGE_FACTOR', rape_df['RR'] / rape_df['RL'])
rape_df.display()

# COMMAND ----------

# Merge the revised definition factor on to the main dataset, and drop unneeded columns
from pyspark.sql.functions import expr
crimes_joined = crimes.join(rape_df, crimes.state_abbr == rape_df.state_abbr, how='left')#.select(rape_df.RL, rape_df.RR, rape_df.DEF_CHANGE_FACTOR)
crimes_joined = crimes_joined.drop('RL', 'RR', 'state_abbr') # takes out both 'state_abbr' columns
crimes_joined=crimes_joined.withColumn("rape_final",expr("case when rape_revised == 0 then rape_legacy * DEF_CHANGE_FACTOR else rape_revised end"))
crimes_joined = crimes_joined.drop('DEF_CHANGE_FACTOR', 'rape_legacy', 'rape_revised')
# crimes_joined.display()
crimes_joined.count()

# COMMAND ----------

crimes_joined.display()

# COMMAND ----------

# Write not supported: Files in repos are currently read-only. If your notebook needs to create a file, consider changing the target to /tmp.
# Hence, write to the DBFS file system; by default it will assume 'DBFS aware'; for future notebooks, leverage DBFS
data_location_root = "/FileStore/datasets/"
data_location = "/FileStore/datasets/processed-dataset/"
crimes_joined.write.format("csv").option('header','true').mode("overwrite").save(data_location)

# Clear out additional files produced in directory to create single CSV file
files = dbutils.fs.ls(data_location)
csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
dbutils.fs.mv(csv_file, data_location.rstrip('/') + ".csv")
dbutils.fs.rm(data_location, recurse = True)

# COMMAND ----------

dbutils.fs.ls(data_location_root)

# COMMAND ----------


