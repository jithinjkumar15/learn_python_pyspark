# Databricks notebook source
simpleData = [(1,"Sagar","CSE","UP",80),
              (2,"Raj","ECE","UP",90),
              (3,"Ravi","EEE","UP",70)]
columns = ["ID","Student_Name","Dept","State","Marks"]
df_1 = spark.createDataFrame(simpleData,columns)

# COMMAND ----------

df_1.show()

# COMMAND ----------

simpleData = [(6,"Ravi","CSE","UP",80),
              (7,"Menon","ECE","UP",90),
              (8,"Nair","EEE","UP",70)]
df_2 = spark.createDataFrame(simpleData,columns)

# COMMAND ----------

df = df_1.union(df_2)
df.show()

# COMMAND ----------


