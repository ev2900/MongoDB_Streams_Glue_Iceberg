## Initial load of MongoDB data to S3 in Apache Iceberg format

The AWS Glue spark job [1_initial_load.py](https://github.com/ev2900/MongoDB_Streams_Glue_Iceberg/blob/main/2_Glue_Iceberg_Initial_Load/1_initial_load.py) will use the file extract of the MongoDB collection that you created via. [1_get_all_documents_from_mongoDB_collection.py](https://github.com/ev2900/MongoDB_Streams_Glue_Iceberg/blob/main/1_Sample_MongoDB_Data/1_get_all_documents_from_mongoDB_collection.py) and it will create an Iceberg table in S3 that is registered with the AWS Glue data catalog. 

This is our initial load of the MongoDB data into iceberg. In subsequent steps we will keep this copy of the data up to date via. the MongoDB change stream and a different Glue job.

Create a Glue job and use the spark code in [1_initial_load.py](https://github.com/ev2900/MongoDB_Streams_Glue_Iceberg/blob/main/2_Glue_Iceberg_Initial_Load/1_initial_load.py) file to complete the initial load of the MongoDB collection. Make sure to read the comments in the spark script. You will need to update some configurations in the script and in the AWS Glue Job configurations.
