# Databricks notebook source
#####imports####

from pyspark.sql.functions import *
from pyspark.sql.types import *

### reading the data ###

employee_df = spark.read.format("csv")\
    .option("header" , "True")\
    .option("inferschema" , "True")\
    .option("mode" , "PERMISSIVE")\
    .load("/FileStore/tables/employee___Sheet1.csv")
employee_df.show()

### printing the schema ###
employee_df.printSchema()

### Creating temporary View ###
employee_df.createOrReplaceTempView("employees_tbl")



# COMMAND ----------

#### aliasing 
employee_df.select(col("id").alias("employee_id")).show()

# COMMAND ----------

###filter or where both are same 

employee_df.filter(col("salary")>=150000).show()

# COMMAND ----------

# with where 
employee_df.where(col("salary")>=150000).show()
