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

# Update the S3 bucket name variable. Do NOT include s3:// prefix or a trailing /
s3_bucket_name = "<s3_bucket_name>"

# You Glue job MUST 
# 1. Use Glue version 4.0
# 2. Job parameter key = --datalake-formats , value = iceberg
# 3. Job parameter key = -- conf            , value = spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions  --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog  --conf spark.sql.catalog.glue_catalog.warehouse=s3://<bucket_name>/  --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog  --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
#    re. ^ (#3) make sure you update <s3_bucket>

# Read in the S3 copy of the data that needs to have the changes applied to it
user_dynamic_frame_0 = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3",
    connection_options= {"paths": ["s3://" + s3_bucket_name + "/initial_load/"]},
    format = "json"
)

user_data_frame_0 = user_dynamic_frame_0.toDF()

# Read in the MongoDB change stream data
change_stream_dynamic_frame_0 = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3",
    connection_options= {"paths": ["s3://" + s3_bucket_name + "/change_stream/"]},
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

# If there are updates/inserts and deletes merge the two dataframes
if len(update_insert.head(1)) > 0:
    # Change the wallTime data type in the update_insert dataframe from a string to a timestamp
    update_insert = update_insert.withColumn("wallTime_timestamp", col('wallTime').cast('timestamp'))
    update_insert = update_insert.select("operationType", "_id", col("wallTime_timestamp").alias("wallTime"), "first_name", "last_name", "email", "gender", "address", "card", "married_status")

    # Union the update_insert dataframe with the delete_w_full_document dataframe
    change_stream_insert_update_delete = update_insert.union(delete_w_full_document)

else:
    change_stream_insert_update_delete = delete_w_full_document
    

# Remove any duplicate records. Taking the latest if there are multiple updates for a single record
change_stream_insert_update_delete.createOrReplaceTempView("tmp_change_stream_insert_update_delete")

# Select only the latest wallTime grouped by _id
no_duplicate_change_stream_insert_update_delete = spark.sql("""SELECT _id AS uniq_id, max(wallTime) AS uniq_wallTime FROM tmp_change_stream_insert_update_delete GROUP BY _id""")

# Join back the dataframe with only the latest update by _id to get the columns other than _id and wallTime
join_conditions = [((no_duplicate_change_stream_insert_update_delete.uniq_id == change_stream_insert_update_delete._id) & (no_duplicate_change_stream_insert_update_delete.uniq_wallTime == change_stream_insert_update_delete.wallTime))]
no_duplicate_change_stream_insert_update_delete_full_record = no_duplicate_change_stream_insert_update_delete.join(change_stream_insert_update_delete, join_conditions, "left")

# Iceberg merg into statement
change_stream_ready_to_merge = no_duplicate_change_stream_insert_update_delete_full_record.select("operationType", "_id", "wallTime", "first_name", "last_name", "email", "gender", "address", "card", "married_status")
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