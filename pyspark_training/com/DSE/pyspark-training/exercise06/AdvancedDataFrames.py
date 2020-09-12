from pyspark.shell import sqlContext

dataPath = "/in/diamond.csv"
df = sqlContext.read.format("com.databricks.spark.csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load(dataPath)

print(df.take(5))

# groupBy operation
df1 = df.groupBy("cut", "color").avg("price")  # a simple grouping

# join operations
df2 = df1\
  .join(df, on='color', how='inner')\
  .select("`avg(price)`", "carat")

# explain dataframe
df2.explain()

# count no of rows in the dataframe
df2.count()

# cache the dataframe
df2.cache()
