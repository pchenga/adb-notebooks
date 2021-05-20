// Databricks notebook source

spark.conf.set("fs.azure.account.key.pcazuresadlsgen2.dfs.core.windows.net", dbutils.secrets.get(scope="azurekv1-scope", key= "adls-password"))

// COMMAND ----------

// DBTITLE 1,Read an adls gen2 location
val df = spark
.read
.option("header","true")
.option("inferSchema","true")
.csv("abfss://input@pcazuresadlsgen2.dfs.core.windows.net/emp1.csv")


df.printSchema()
display(df)

// COMMAND ----------

// DBTITLE 1,Write data to the adls


val finalDf =  df.select("ename", "salary", "bonus")

finalDf.write.format("parquet").save("abfss://input@pcazuresadlsgen2.dfs.core.windows.net/parquet")


// COMMAND ----------

val parDF = spark.read.parquet("abfss://input@pcazuresadlsgen2.dfs.core.windows.net/parquet")


display(parDF)