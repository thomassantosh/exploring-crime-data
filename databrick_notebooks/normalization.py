# Databricks notebook source
# MAGIC %md
# MAGIC ### Normalizing for population changes, how have various crimes grown or declined?

# COMMAND ----------

# Total rows that should be outputted after cleaning: 2,184 (42 years * 52 states + TOTAL entries = 2,184)
crimes = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/FileStore/datasets/processed-dataset.csv")
crimes.count()

# COMMAND ----------

crimes.display()

# COMMAND ----------

# Vectorize for upcoming standardization, normalization
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=['population', 'violent_crime', 'homicide', 'robbery','aggravated_assault', 'property_crime', 'burglary', 'larceny', 'motor_vehicle_theft', 'rape_final'],outputCol='statistics')
crime_vector = assembler.transform(crimes)
crime_vector.display()

# COMMAND ----------

from pyspark.ml.feature import Normalizer
import pyspark.sql.functions as F
from functools import reduce  # For Python 3.x
from pyspark.sql.types import ArrayType, DoubleType

# Normalization by inf or 'max norm', where the non-normalized max number of the attributes is taken as '1', every other value expressed as a fraction of this number
normalizer = Normalizer(inputCol='statistics', outputCol='stat_outputs', p=float("inf"))
nd = normalizer.transform(crime_vector)
# nd = nd.select('year', 'state_name','statistics', 'stat_outputs')
# nd.display()

# Create a header dataframe
header_df = nd.select('year', 'state_name')
header_df = header_df.withColumn('row_id',F.monotonically_increasing_id())

# Split the vector into columns, and rename to the proper names
def split_array_to_list(col):
    def to_list(v):
        return v.toArray().tolist()
    return F.udf(to_list, ArrayType(DoubleType()))(col)

split_nd = nd.select(split_array_to_list(F.col("stat_outputs")).alias("split_int")).select([F.col("split_int")[i] for i in range(10)])

# Rename column names
old_col_names = split_nd.schema.names
new_col_names = ['population', 'violent_crime', 'homicide', 'robbery','aggravated_assault', 'property_crime', 'burglary', 'larceny', 'motor_vehicle_theft', 'rape_final']
df = reduce(lambda split_nd, idx: split_nd.withColumnRenamed(old_col_names[idx], new_col_names[idx]), range(len(old_col_names)), split_nd)
df = df.withColumn('row_id',F.monotonically_increasing_id())

# Join both dataframes
new_df = header_df.join(df, header_df.row_id == df.row_id)#.select('header_df.*')
new_df = new_df.select('year', 'state_name','population', 'violent_crime', 'homicide', 'robbery','aggravated_assault', 'property_crime', 'burglary', 'larceny', 'motor_vehicle_theft', 'rape_final')
new_df.display()
# new_df.count()

# COMMAND ----------

# This cell is intended to be interactive, and produce line charts for the various crimes
total_df = new_df.filter( new_df['state_name'] == "TOTAL")
total_df.display()
# new_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### In various 10 year time blocks (1980, 1990, 2000, 2010 and 2020), which states have been in the Top 5 across various crimes?

# COMMAND ----------

# Top 5 states ranking definition
def top_5_states(df=None, time_period=None, crime_list=None):
    for i,v in enumerate(time_period):
        for j,k in enumerate(crime_list):
            print(f"Period is: {v}")
            print(f"Crime is: {k}")
            temp_df = df.filter( df['year'] == v).filter( df['state_name'] != "TOTAL").select('year', 'state_name', k).orderBy(df[k].desc()).limit(5)
#             print(temp_df.show())
            temp_df.display()

# COMMAND ----------

# On normalized data
# This cell is intended to be interactive, and produce line charts for the various crimes
# time_periods = [x.year for x in crimes.select('year').distinct().collect()]
time_periods = ['1980', '1990', '2000', '2010', '2020']
# crime_list = ['violent_crime', 'homicide', 'robbery','aggravated_assault', 'property_crime', 'burglary', 'larceny', 'motor_vehicle_theft', 'rape_final']
crime_list = ['violent_crime', 'property_crime', 'burglary', 'larceny']
top_5_states(df=new_df, time_period=time_periods, crime_list=crime_list)

# COMMAND ----------

# On raw data values
# This cell is intended to be interactive, and produce line charts for the various crimes
# time_periods = [x.year for x in crimes.select('year').distinct().collect()]
time_periods = ['1980', '1990', '2000', '2010', '2020']
crime_list = ['violent_crime', 'homicide', 'robbery','aggravated_assault', 'property_crime', 'burglary', 'larceny', 'motor_vehicle_theft', 'rape_final']
top_5_states(df=crimes, time_period=time_periods, crime_list=crime_list)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Normalize based on Standard Scaler (for comparison across all states)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler

# Standard scaler for specific time period
def standard_scaler_normalization(df=None, time_period=None):
    df = df.filter( df['year'] == '2020').filter( df['state_name'] != "TOTAL")
    assembler = VectorAssembler(inputCols=['violent_crime', 'homicide', 'robbery','aggravated_assault', 'property_crime', 'burglary', 'larceny', 'motor_vehicle_theft', 'rape_final'],outputCol='statistics')
    td = assembler.transform(df)
    scaler = StandardScaler(inputCol='statistics', outputCol='stats_scaled',withStd=True, withMean=True)
    scaler_model = scaler.fit(td)
    td_scaled = scaler_model.transform(td)
    
    # Create header, for later joining
    tds_header = td_scaled.select('year','state_name')
    tds_header = tds_header.withColumn('row_id',F.monotonically_increasing_id())
    
    # Split the vector into columns, and rename to the proper names
    def split_array_to_list(col):
        def to_list(v):
            return v.toArray().tolist()
        return F.udf(to_list, ArrayType(DoubleType()))(col)

    tds = td_scaled.select(split_array_to_list(F.col("stats_scaled")).alias("var")).select([F.col("var")[i] for i in range(9)])

    # # # Rename column names
    old_col_names = tds.schema.names
    new_col_names = ['violent_crime', 'homicide', 'robbery','aggravated_assault', 'property_crime', 'burglary', 'larceny', 'motor_vehicle_theft', 'rape_final']
    mdf = reduce(lambda tds, idx: tds.withColumnRenamed(old_col_names[idx], new_col_names[idx]), range(len(old_col_names)), tds)
    mdf = mdf.withColumn('row_id',F.monotonically_increasing_id())
    # df.display()

    # Join both dataframes
    new_df = tds_header.join(mdf, tds_header.row_id == mdf.row_id)
    return new_df#.display()

df = standard_scaler_normalization(df = crimes, time_period='2020')
df.display()
