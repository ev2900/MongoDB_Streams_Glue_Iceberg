# 
# 
# 

import os
import pymongo
from bson.json_util import dumps

client = pymongo.MongoClient('<mongo-db-connection-string>')

change_stream = client.sampleDB.user.watch()

counter = 0

for change in change_stream:

    del change['clusterTime']
    del change['wallTime']

    change['id'] = change['_id']
    del change['_id']

    change['documentKey']['_id'] = str(change['documentKey']['_id']).replace("ObjectId(","").replace(")","")

    if 'fullDocument' in change:
        change['fullDocument']['_id'] = str(change['fullDocument']['_id']).replace("ObjectId(","").replace(")","")

    if change['operationType'] == 'update':
        if len(change['updateDescription']['removedFields']) == 0:
            del change['updateDescription']['removedFields']

    if change['operationType'] == 'update':
        if len(change['updateDescription']['truncatedArrays']) == 0:
            del change['updateDescription']['truncatedArrays']

    change = str(change).replace("'", '"').replace("True", "true").replace("False","false")

    output_file = open("change_stream_output_" + str(counter) + ".json", "w")

    output_file.write(change)

    output_file.close()

    print("file change_" + str(counter) + ".json created")

    counter = counter + 1