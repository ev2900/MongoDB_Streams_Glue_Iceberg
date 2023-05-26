from bson.json_util import dumps
from pymongo import MongoClient
import json

mongoDB_connection_string_file = open("0_mongoDB_connection_string.txt", "r")
mongoDB_connection_string = mongoDB_connection_string_file.readline()
mongoDB_connection_string_file.close()

database_name = "sampleDB"

client = MongoClient(mongoDB_connection_string)

db = client[database_name]

# db.<collection_name>
cursor = db.user

mongoDB_dump_file_content = ""

for document in cursor.find():

    document['_id'] = str(document['_id'])

    document = str(document).replace("'", '"').replace("True", "true").replace("False","false") + "\n"

    mongoDB_dump_file_content = mongoDB_dump_file_content + document


mongoDB_dump_file = open("2_mongoDB_dump.json", "w")
mongoDB_dump_file.write(mongoDB_dump_file_content.rstrip())
mongoDB_dump_file.close()