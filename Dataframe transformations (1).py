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

# COMMAND ----------

#multiple filter 
employee_df.filter((col("salary")>=150000)& (col("age")<18)).show()


# COMMAND ----------

# let's suppose we don';t have any last name sometimes so maintain clean records we use literals
employee_df.select("*" , lit("kumar").alias("last_name")).show()
#see a new column appears and it's value is kumar cause we set the lit 


# COMMAND ----------

employee_df.select( expr("concat(name,last_name)")).show()

# COMMAND ----------

# adding columns -- with columns - agar columns hogaa toh overwrite else new bann jaega 
employee_df.withColumn("surname", lit("shukla")).show()

# COMMAND ----------

employee_df.withColumnRenamed("id","employee_id").show() ##renamingcolumnwith withcolumn function

# COMMAND ----------

#casting data types -- changing data type of columns let's seee
employee_df.withColumn("id",col("id").cast("string")).printSchema()

# COMMAND ----------

# removing a column
employee_df.drop("id", col("name")).show()

# COMMAND ----------

spark.sql("""
          
          
       select * , "shukla" as last_name, concat(name,last_name ) as full_name, id as employee_id, cast(id as string)  from employees_tbl where salary>150000 and age <18   
          
          
          """).printSchema()
