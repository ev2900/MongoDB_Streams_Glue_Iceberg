from bson.json_util import dumps
from pymongo import MongoClient
import json

client = MongoClient("<mongo-db-connection-string>")

db = client["<mongo-database-name>"]

cursor = db.<mongodb-collection-name>

mongoDB_dump_file = open("2_mongoDB_dump.json", "w")

for document in cursor.find():

    document['_id'] = str(document['_id']).replace("ObjectId(","").replace(")","")

    document = str(document).replace("'", '"').replace("True", "true").replace("False","false") + "\n"

    mongoDB_dump_file.write(document)

mongoDB_dump_file.close()