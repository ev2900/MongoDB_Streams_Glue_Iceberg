import os
import pymongo
from bson.json_util import dumps
from bson.objectid import ObjectId

mongoDB_connection_string_file = open("0_mongoDB_connection_string.txt", "r")
mongoDB_connection_string = mongoDB_connection_string_file.readline()
mongoDB_connection_string_file.close()

client = pymongo.MongoClient(mongoDB_connection_string)

# client.<database_name>.<collection_name>
change_stream = client.sampleDB.user.watch()

counter = 0

for change in change_stream:

    if change['operationType'] != "delete":

        # Look up full record + add the operationType field
        update_insert = client.sampleDB.user.find_one({"_id": ObjectId(change['documentKey']['_id'])})
        
        if update_insert != None:

            update_insert["operationType"] = change["operationType"]
            # update_insert["clusterTime"] = str(change["clusterTime"])    
            update_insert["wallTime"] = str(change["wallTime"])

            # Clean up json
            update_insert["_id"] = str(change['documentKey']['_id'])

            update_insert = str(update_insert).replace("'", '"').replace("True", "true").replace("False","false")

            # Write to file
            output_file = open("change_stream_output_" + str(counter) + ".json", "w")
            output_file.write(update_insert)
            output_file.close()

            print("file change_" + str(counter) + ".json created")

        else:
            print("Object _id " + change['documentKey']['_id'] + "not found")

    elif change['operationType'] == "delete":

        delete = {}

        delete["operationType"] = change["operationType"]
        delete["_id"] = str(change['documentKey']['_id'])
        delete["wallTime"] = str(change["wallTime"])

        # Clean up json
        delete = str(delete).replace("'", '"')

        # Write to file
        output_file = open("change_stream_output_" + str(counter) + ".json", "w")
        output_file.write(delete)
        output_file.close()

        print("file change_" + str(counter) + ".json created")

    counter = counter + 1