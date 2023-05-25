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

dynamic_frame_0 = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3",
    connection_options= {"paths": ["s3://mongodb-dhdkre3/initial_load/2_mongoDB_dump.json"]},
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