import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read in the S3 copy of the data that needs to have the changes applied to it
user_dynamic_frame_0 = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3",
    connection_options= {"paths": ["s3://mongodb-dhdkre3/initial_load/2_mongoDB_dump.json"]},
    format = "json"
)

user_data_frame_0 = user_dynamic_frame_0.toDF()

# Read in the MongoDB change stream data
change_stream_dynamic_frame_0 = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3",
    connection_options= {"paths": ["s3://mongodb-dhdkre3/change_stream/"]},
    format = "json"
)

change_stream_data_frame_0 = change_stream_dynamic_frame_0.toDF()

# Split the MongoDB change stream data into 2 dataframes. 1 for updates + inserts. 1 for deletes
update_insert = change_stream_data_frame_0.filter((change_stream_data_frame_0.operationType == "update") | (change_stream_data_frame_0.operationType == "insert"))

delete = change_stream_data_frame_0.filter(change_stream_data_frame_0.operationType == "delete")

# Re-name the columns in the delete dataframe. Pre-fix each column name with cs_
delete = delete.select([functions.col(column_name).alias("cs_" + column_name) for column_name in delete.columns])

# Join the delete dataframe to the user_dataframe. The purpose of this is to get full records for deleted data. The MondoDB change stream only provides the document ID for the deleted document. It does not provide the entire document
delete_w_full_document = delete.join(user_data_frame_0, delete.cs__id == user_data_frame_0._id, "left")

# Add a column wallTime_timestamp that has the wall time as a timestamp value instead of a string
delete_w_full_document = delete_w_full_document.withColumn("wallTime_timestamp", col('cs_wallTime').cast('timestamp'))

# Select only the columns needed from the previous joined dataframe
delete_w_full_document = delete_w_full_document.select(col("cs_operationType").alias("operationType"), "_id", col("wallTime_timestamp").alias("wallTime"), "first_name", "last_name", "email", "gender", "address", "card", "married_status")

# Change the wallTime data type in the update_insert dataframe from a string to a timestamp
update_insert = update_insert.withColumn("wallTime_timestamp", col('wallTime').cast('timestamp'))
update_insert = update_insert.select("operationType", "_id", col("wallTime_timestamp").alias("wallTime"), "first_name", "last_name", "email", "gender", "address", "card", "married_status")

# Union the update_insert dataframe with the delete_w_full_document dataframe
change_stream_ready_to_merge = update_insert.union(delete_w_full_document).orderBy("wallTime")

# Iceberg merg into statement
change_stream_ready_to_merge.createOrReplaceTempView("updates")

query = (""" 
MERGE INTO glue_catalog.iceberg.user as t 
USING updates s 
ON t._id = s._id
WHEN MATCHED AND s.operationType = 'update' THEN UPDATE SET t.first_name = s.first_name, t.last_name = s.last_name, t.email = s.email, t.gender = s.gender, t.address = s.address, t.card = s.card, t.married_status = s.married_status
WHEN MATCHED AND s.operationType = 'delete' THEN DELETE
WHEN NOT MATCHED AND s.operationType = 'insert' THEN INSERT (_id, first_name, last_name, email, gender, address, card, married_status) VALUES (s._id, s.first_name, s.last_name, s.email, s.gender, s.address, s.card, s.married_status) 
""")

spark.sql(query)

job.commit()