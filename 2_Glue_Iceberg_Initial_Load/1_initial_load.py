import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

dynamic_frame_0 = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3",
    connection_options= {"paths": ["s3://" + s3_bucket_name "/initial_load/"]},
    format = "json"
)

data_frame_0 = dynamic_frame_0.toDF()

data_frame_0.createOrReplaceTempView("tmp_data_frame_0")

query = """
CREATE TABLE glue_catalog.iceberg.user
USING iceberg
AS SELECT * FROM tmp_data_frame_0
"""

spark.sql(query)

job.commit()