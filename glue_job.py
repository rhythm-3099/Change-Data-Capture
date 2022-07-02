from awsglue.utils import getResolvedOptions
import sys
from pyspark.sql.functions import when
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv,['s3_target_path_key','s3_target_path_bucket'])
bucket = args['s3_target_path_bucket']
fileName = args['s3_target_path_key']

print(bucket, fileName)

spark = SparkSession.builder.appName("CDC").getOrCreate()
inputFilePath = f"s3a://{bucket}/{fileName}"
finalFilePath = f"s3a://cdc-ouput-pyspark/output"

if "LOAD" in fileName:
  # case: full load
    input_file_df = spark.read.csv(inputFilePath)
    input_file_df = input_file_df.withColumnRenamed("_c0","id").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    input_file_df.write.mode("overwrite").csv(finalFilePath)
else:
  # case: replication ongoing
    updated_df = spark.read.csv(inputFilePath)
    updated_df = updated_df.withColumnRenamed("_c0","action").withColumnRenamed("_c1","id").withColumnRenamed("_c2","FullName").withColumnRenamed("_c3","City")
    final_file_df = spark.read.csv(finalFilePath)
    final_file_df = final_file_df.withColumnRenamed("_c0","id").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    
    for row in updated_df.collect(): 
      if row["action"] == 'U':
          # case: update
        final_file_df = final_file_df.withColumn("FullName", when(final_file_df["id"] == row["id"], row["FullName"]).otherwise(final_file_df["FullName"]))      
        final_file_df = final_file_df.withColumn("City", when(final_file_df["id"] == row["id"], row["City"]).otherwise(final_file_df["City"]))
        
          # case: insert
        insertedRow = [list(row)[1:]]
        columns = ['id', 'FullName', 'City']
        newdf = spark.createDataFrame(insertedRow, columns)
        final_file_df = final_file_df.union(newdf)
    
      if row["action"] == 'D':
          # case: delete
        final_file_df = final_file_df.filter(final_file_df.id != row["id"])
        
    final_file_df.write.mode("overwrite").csv(finalFilePath)   