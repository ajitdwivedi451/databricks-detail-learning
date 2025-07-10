# Databricks notebook source
df = spark.read.option('inferschema', True).option('header', True).csv('/FileStore/tables/BigMart_Sales.csv')


# COMMAND ----------

from pyspark.sql.functions import *
df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' '))

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Array Contains

# COMMAND ----------

df_exp.withColumn('outlet_Type_flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group BY

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find avg amount by using group by

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### applying group by two column(Find sum of item Type as weel Outlet_Size)

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP'), avg('Item_MRP').alias('Item_MRP_avg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collection_List

# COMMAND ----------

#Create a data from 
data = [('User1','Book1'),
        ('User2','Book1'),
        ('User3','Book1'),
        ('User1','Book2'),
        ('User3','Book2'),
        ('User1','Book3'),
        ('User4','Book1')] 
schema = 'user String, Book String'
df_cl = spark.createDataFrame(data, schema)

# COMMAND ----------

df_cl2 = spark.createDataFrame(data = [('User1','Book1'),
        ('User2','Book1'),
        ('User3','Book1'),
        ('User1','Book2'),
        ('User3','Book2'),
        ('User1','Book3'),
        ('User4','Book1')], schema = 'user String, book String')
df_cl2.display()

# COMMAND ----------

df_cl.display()

# COMMAND ----------

df_cl.groupBy('user').agg(collect_list('Book')).display()

# COMMAND ----------

df_cl.groupBy('user').agg(collect_set('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PIVOT

# COMMAND ----------

 df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display() 

# COMMAND ----------

df.select('Item_Type','Outlet_Size','Item_MRP').groupBy('Item_type','Outlet_Size').agg(avg('Item_MRP').alias('Pura Dam')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### When-Otherwise

# COMMAND ----------

df = df.withColumn('Veg_Flag', when(col('Item_Type')=='Meat', 'NonVeg').otherwise('Veg')).display()

# COMMAND ----------

df.withColumn('Veg_Exp_Flag', when(((col('Veg_Flag')=='Veg') & (col('Item_MRP') >100)), 'Veg Expensive')\
                             .when((col('Veg_Flag')=='Veg') & (col('Item_MRP') <100), 'Veg Inexpensive')\
                             .otherwise('Non Veg')).display()

# COMMAND ----------


