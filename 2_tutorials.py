# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format('csv').option('inferschema', True).option('header', True).load('/FileStore/tables/BigMart_Sales.csv')


# COMMAND ----------

df= df.withColumn('curr_date',current_date())


# COMMAND ----------

df = df.withColumn('week_after',date_add('curr_date', 7))

# COMMAND ----------

df = df.withColumn('date_before', date_add('curr_date',-7))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Datediff and dateformat

# COMMAND ----------

df = df.withColumn('Date_Diff',datediff('week_after','curr_date'))
df.display()

# COMMAND ----------

df = df.withColumn('week_after',date_format('curr_date','MM-yyyy-dd'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROPING NULL(by droping or filling)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dropping Case(any,All,subset)

# COMMAND ----------

df.dropna('any')
df.display()


# COMMAND ----------

df.filter(col('Item_weight').isNull() & col('Item_Identifier').isNull()).display()

# COMMAND ----------

df.dropna('all')# DROP RECORD WHOSE ALL COLUMNA ARE NULL
df.display()

# COMMAND ----------

df.dropna(subset = ['Item_weight']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Nulls by filling null by some value (based on data type of column, subset, and multiples column using  )

# COMMAND ----------

df.fillna('NA').display() # for column with String Datatype 

# COMMAND ----------

df.fillna(50000000).display() # inter data type fill by 5000000

# COMMAND ----------

df.fillna({'Item_weight' : 120000, 'Outlet_Size' : 'Unknow'}).display() 

# COMMAND ----------

df.fillna('Main nahi janta',subset='Outlet_Size').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spliting and Indexing
# MAGIC ### 

# COMMAND ----------

from pyspark.sql.functions import split
df = spark.createDataFrame([
    ('one Atwo Bthree C', '[ABC]', 2),
    ('1A2B3C', '[1-9]+', 1),
    ('aa2bb3cc4', '[1-9]+', -1)], ['s', 'p', 'l']).display()
df.select('*',split('s', ' ')).display()

# COMMAND ----------

#import pyspark.sql.functions as sf
#from pyspark.sql import SparkSession
#from pyspark.sql.functions import split,col 
df = spark.createDataFrame([
    ('oneAtw oBth reeC', '[ABC]', 2),
    ('1A2B3C', '[1-9]+', 1),
    ('aa2bb3cc4', '[1-9]+', -1)], ['s', 'p', 'l'])
df2 = df.withColumn('s', split(df['s'], '[ABC ]')).display()

# COMMAND ----------

df = spark.read.format('csv').option('inferschema', True).option('header', True).load('/FileStore/tables/BigMart_Sales.csv')


# COMMAND ----------

df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Explode will work on list datatype column  and explode of list element as seprate row

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------


