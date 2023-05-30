## Load Sample Data to MongoDB

If you do not already have a MongoDB collection you can create one with sample data provided in [0_mongoDB_input.json](https://github.com/ev2900/MongoDB_Streams_Glue_Iceberg/blob/main/1_Sample_MongoDB_Data/0_mongoDB_input.json). [MongoDB Compass](https://www.mongodb.com/products/compass) can be an easy way to upload the file into a MongoDB collection. 

The [insert document](https://www.mongodb.com/docs/compass/current/documents/insert/) section of the compass documentation provides instructions on how to upload a JSON file into a collection.

## Dump All Documents in the Collection into a File

We need to extract the entire MongoDB collection into a file. This file should subsequently be uploaded to S3 and will be used by the Glue job in the next step. 

This extraction is a one time process. Ultimately we will use the MongoDB change stream and another Glue job to keep this extract in sync with the actual collection.

The [1_get_all_documents_from_mongoDB_collection.py](https://github.com/ev2900/MongoDB_Streams_Glue_Iceberg/blob/main/1_Sample_MongoDB_Data/1_get_all_documents_from_mongoDB_collection.py) will connect to a specified collection and extract all of the data into a JSON file. Make sure to read the comments in the python script. You will need to update some configurations.

If you are using the sample data ( ie. [0_mongoDB_input.json](https://github.com/ev2900/MongoDB_Streams_Glue_Iceberg/blob/main/1_Sample_MongoDB_Data/0_mongoDB_input.json) ) the output of the [1_get_all_documents_from_mongoDB_collection.py](https://github.com/ev2900/MongoDB_Streams_Glue_Iceberg/blob/main/1_Sample_MongoDB_Data/1_get_all_documents_from_mongoDB_collection.py) is already provided as [2_mongoDB_dump.json](https://github.com/ev2900/MongoDB_Streams_Glue_Iceberg/blob/main/1_Sample_MongoDB_Data/2_mongoDB_dump.json) 
