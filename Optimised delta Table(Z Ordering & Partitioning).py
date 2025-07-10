# Databricks notebook source
display(dbutils.fs.ls('/databricks-datasets/definitive-guide/data/retail-data/all/'))

# COMMAND ----------

# MAGIC %fs cp dbfs:/databricks-datasets/definitive-guide/data/retail-data/all/online-retail-dataset.csv dbfs:/data/input/sales/sales.csv

# COMMAND ----------

df = spark.read.format('csv').load('dbfs:/databricks-datasets/definitive-guide/data/retail-data/all/online-retail-dataset.csv', header ='true', inferSchema ='true')
df.createOrReplaceTempView('retail_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from retail_data

# COMMAND ----------

# MAGIC %fs head dbfs:/data/input/sales/sales.csv

# COMMAND ----------

df_new = spark.read.format('csv').load('dbfs:/data/input/sales/sales.csv', header='true', inferSchema ='true')
df_new.repartition(16).write.format("delta").mode('overwrite').option("path", "dbfs:/data/output/sales_delta/").saveAsTable('sales_delta')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SHOW TABLES IN spark_catalog.default

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table sales_delta

# COMMAND ----------

#  %fs rm -r dbfs:/data/output/sales_delta/sales.csv

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/data/output/sales_delta/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_delta where InvoiceNo = '540176'

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(InvoiceNo),max(InvoiceNo) ,_metadata.file_name from sales_delta group by _metadata.file_name order by min(InvoiceNo)

# COMMAND ----------

# MAGIC %md
# MAGIC first time min max is over lapping so i need to read all file for scanning which takes more time and resources.

# COMMAND ----------

# MAGIC %md
# MAGIC above data is uneven distributed so it have to check all the files for scan data with given invoice id so it take more time so we need optimisation

# COMMAND ----------

# Since we have less size of data now but spark by defalut partion by 1 gb size so we nned to partined max size by 4mb for now
spark.conf.set('spark.databricks.delta.optimize.maxFileSize', 64*1024*8)

# COMMAND ----------

# MAGIC %md
# MAGIC Optimised the table now by z oderring to skip and scan best 

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize sales_delta zorder by (InvoiceNo)

# COMMAND ----------

# MAGIC %md
# MAGIC re run the query of min max 

# COMMAND ----------

# MAGIC %md
# MAGIC Re run same sql query to get identifed that how much time it takes to execute the query

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_delta where InvoiceNo = '540176'
# MAGIC -- this query has been optimised because it takes only .04 sec and read only 1 file while same query before z ordering it has to read 16 file to scan the value. 

# COMMAND ----------

# MAGIC %md
# MAGIC Check optmised cell metric

# COMMAND ----------

# MAGIC %md
# MAGIC Multi column z ordering 

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE sales_delta ZORDER BY (Country,InvoiceNo)
# MAGIC -- if we increase no of column in the zorder the i may have to read more file which can increase time 

# COMMAND ----------

# MAGIC %sql
# MAGIC select country, min(InvoiceNo),max(InvoiceNo) ,_metadata.file_name from sales_delta group by country,_metadata.file_name order by country,min(InvoiceNo)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_delta where InvoiceNo = '540176' and Country= "United Kingdom"

# COMMAND ----------

# MAGIC %md
# MAGIC ###better way is partion based on country and then z order based on country
# MAGIC so performance became much better

# COMMAND ----------

# MAGIC %md
# MAGIC ### partision column basd on country

# COMMAND ----------

df_new_column_partion = spark.read.format('csv').load('dbfs:/data/input/sales/sales.csv', header='true', inferSchema ='true')
df_new.write.partitionBy('Country').format("delta").mode('overwrite').option("path", "dbfs:/data/output/sales_delta_partion/").saveAsTable('sales_delta_partion')

# COMMAND ----------

# %fs rm -r dbfs:/data/output/sales_delta_partion/

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table sales_delta_partion

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/data/output/sales_delta_partion/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_delta_partion where InvoiceNo = '536389' 

# COMMAND ----------

# MAGIC %sql
# MAGIC --  if we want to select partion by country and optmised 
# MAGIC -- OPTIMIZE sales_delta_partion where Country ='Australia' ZORDER BY (InvoiceNo)
# MAGIC -- Optmised whole column
# MAGIC OPTIMIZE sales_delta_partion ZORDER BY (InvoiceNo)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_delta_partion where InvoiceNo = '536389' 
