# Databricks notebook source
simpleData = [(1,["Sagar","CSE","UP",80]),
              (2,["Raj","ECE","UP",90]),
              (3,["Ravi","EEE","UP",70])]
columns = ["ID","Details"]
df_1 = spark.createDataFrame(simpleData,columns)

# COMMAND ----------

df_1.show()

# COMMAND ----------

from pyspark.sql.functions import col,explode
df = df_1.select(col("ID"),explode("Details"))

# COMMAND ----------

df1= df_1["ID",explode("Details")]

# COMMAND ----------

display(df1)

# COMMAND ----------


