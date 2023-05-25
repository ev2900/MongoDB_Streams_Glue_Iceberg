import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read in existing user data
user_data_frame_0 = spark.read.format("iceberg").load("glue_catalog.iceberg.user")
user_data_frame_0.createOrReplaceTempView("tmp_user_dynamic_frame_0")

# Read in change stream data
change_stream_dynamic_frame_0 = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3",
    connection_options= {"paths": ["s3://mongodb-dhdkre3/change_stream/"]},
    format = "json"
)

change_stream_data_frame_0 = change_stream_dynamic_frame_0.toDF()
change_stream_data_frame_0.createOrReplaceTempView("tmp_change_stream_data_frame_0")

query = """
CREATE TABLE glue_catalog.iceberg.user_change_stream
USING iceberg
AS SELECT * FROM tmp_change_stream_data_frame_0
"""
spark.sql(query)

# Merge the change stream data into the existing user data
'''
spark.sql(""" 
    
   
   
   
   
""")
'''

job.commit()