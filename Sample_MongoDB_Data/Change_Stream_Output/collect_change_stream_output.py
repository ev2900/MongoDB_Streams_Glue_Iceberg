# 
# 
# 

import os
import pymongo
from bson.json_util import dumps

client = pymongo.MongoClient('<mongo-db connection string>')

change_stream = client.sample_db.users.watch()

counter = 10

for change in change_stream:

    # print(dumps(change))

    output_file = open("change_" + str(counter) + ".json", "w")

    output_file.write(dumps(change))

    output_file.close()

    print("file change_" + str(counter) + ".json created")

    counter = counter + 1