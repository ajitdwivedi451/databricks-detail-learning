# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

# MAGIC %md
# MAGIC import csv data

# COMMAND ----------

df = spark.read.format('csv').option('inferschema', True).option('header', True).load('/FileStore/tables/BigMart_Sales.csv')


# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ######### when inferSchema is not true

# COMMAND ----------

df_withourinferSchma = spark.read.format('csv').option('header', True).load('/FileStore/tables/BigMart_Sales.csv')

df_withourinferSchma.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ######Read JSON files

# COMMAND ----------

df_json = spark.read.format('json').option('header', True)\
.load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

from pyspark.sql.functions import *
df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

#df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER

# COMMAND ----------

df.filter(col('Item_Fat_Content') == 'Regular').display()

# COMMAND ----------

df.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio Three (Outlet size col is null and Outlet_Location_Type is either Tier 1 or Tier 2)

# COMMAND ----------



# COMMAND ----------

df.filter((col('Outlet_Size').isNull() &(col('Outlet_Location_Type').isin('Tier 1','Tier 2')))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC # withColumnRename

# COMMAND ----------

df.withColumnRenamed('Item_weight','Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### withColumn Scenerio-1(Create new column and add a litral value)

# COMMAND ----------

df.withColumn('flag',lit('new')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### withColumn Scenerio-2(Create new column and add a litral value)

# COMMAND ----------

df.withColumn('Multiply', col('Item_weight') * col('Item_Mrp')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenerio -2

# COMMAND ----------

df.withColumn('Item_fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg"))\
    .withColumn('Item_fat_Content',regexp_replace(col('Item_Fat_Content'),"Low Fat","Lf")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### typeCasting

# COMMAND ----------

df.withColumn('Item_Weight', col('Item_Weight').cast(StringType())).display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### sort- scenerio 1

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()


# COMMAND ----------

df.sort(col('Item_Weight').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sort-s3 list of column

# COMMAND ----------

df.sort(["Item_Weight","Item_Visibility"],ascending =[0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC sorting on itwm weight in descending and visibility on ascending order

# COMMAND ----------

df.sort(["Item_Weight","Item_Visibility"], ascending = [0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIMIT

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DROP

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop-Scenerio 2(Multiple column drop)

# COMMAND ----------

df.drop('Item_Visibility','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Duplicates (Most Important)

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Use Drop_duplicates for particular column 

# COMMAND ----------

df.drop_duplicates(subset=["Item_Type"]).display()

# COMMAND ----------

df.distinct().display() # It will do the same thing as dropDuplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union and Union BYNAME (using df1 and df2)

# COMMAND ----------

from pyspark.sql import SparkSession
data = [('1','kad'),('2','sid')]
schema = 'id String', 'Name String'
data1 =[('3','manoj'),('4','suresh')]
schema1 = 'id String', 'Name STRING'

df1 = spark.createDataFrame(data, schema)
df2 = spark.createDataFrame(data1, schema1)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###if data frame had unorderd column

# COMMAND ----------

from pyspark.sql import SparkSession
data = [('kad','1'),('sid','2')]
schema = 'Name String', 'id String'
data1 =[('3','manoj'),('4','suresh')]
schema1 = 'id String', 'Name STRING'

df3 = spark.createDataFrame(data, schema)
df2 = spark.createDataFrame(data1, schema1)

# COMMAND ----------

df2.union(df3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###unionByName (it will union according to the despite of column order)

# COMMAND ----------

df2.unionByName(df3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### String Functions initcap(),lower(),upper()

# COMMAND ----------

from pyspark.sql.functions import *
df.select(upper(col('Item_Type')), col('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###current_date(),date_add(),datesub()

# COMMAND ----------

df = spark.read.format('csv').option('inferschema', True).option('header', True).load('/FileStore/tables/BigMart_Sales.csv')


# COMMAND ----------

df= df.withColumn('curr_date',current_date())

# COMMAND ----------

df = df.withColumn('week_after',date_add('curr_date', 7))

# COMMAND ----------

#df.display()

# COMMAND ----------

df = df.withcolumn('week_before',date_sub('curr_date',7))

# COMMAND ----------


