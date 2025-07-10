# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format('csv').option('inferschema','True').option('header', True).csv('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### When Otherwise

# COMMAND ----------

df= df.withColumn('Veg_Flag', when(col('Item_Type')=='Meat', 'NonVeg').otherwise('Veg'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('Veg_Exp_Flag', when((col('Veg_Flag')=='Veg') & (col('Item_MRP') >100), 'Veg Expensive').otherwise('Veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joins

# COMMAND ----------

data1 = [('1','Ajit','d01'),
        ('2','Akash','d02'),
        ('3','Manoj','d03'),
        ('4','Ajay','d03'),
        ('5','Aman','d04'),
        ('6','Golu','d05'),
        ('7','Mahendra','d07')]
schema1 = 'emp_id String, emp_name String, dept_id String'

df1 = spark.createDataFrame(data1, schema1)

data2 = [('d01','HR'),
         ('d02','Marketing'),
         ('d03','Accounts'),
         ('d05','data scientist'),
         ('d06','It'),
         ('d07','Finance'),
         ('d08','Sales'),
         ('d09','data-engineering'),
         ('d10',' data analyst')
         ]
schema2 = 'dept_id String, dept_name String'
df2 = spark.createDataFrame(data2, schema2)


# COMMAND ----------

df1.display()

# COMMAND ----------

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner Join

# COMMAND ----------

df1.join(df2,df1['dept_id'] == df2['dept_id'], 'inner').display()


# COMMAND ----------

df1.join(df2, df1.dept_id == df2.dept_id,'left').display()

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti Join(first df ka row which does not match with the second df)

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Function (Important)
# MAGIC ###  .Row Number()

# COMMAND ----------

display(df)

# COMMAND ----------


