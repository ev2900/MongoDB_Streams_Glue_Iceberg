# change_0, change_1 are pretty formatted update
# change_10, change_11 are pretty formatted delete
# change_12 are pretty formatted insert

import os
import pymongo
from bson.json_util import dumps

client = pymongo.MongoClient('<mongoDB-connection-string>')

change_stream = client.sample_airbnb.listingsAndReviews.watch()

counter = 12

for change in change_stream:

    # print(dumps(change))

    output_file = open("change_" + str(counter) + ".json", "w")

    output_file.write(dumps(change))

    output_file.close()

    print("file change_" + str(counter) + ".json created")

    counter = counter + 1