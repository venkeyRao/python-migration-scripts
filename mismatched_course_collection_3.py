from pymongo import MongoClient
from bson.objectid import ObjectId

client = MongoClient()

source_connection = client['naps_production_source_29-01-2019']['courses']

target_collection = client['mismatch']['courses_mismatched_complete']

name_mismatch = client['mismatch']['mismatch_trade_name']

source_establishment = client['naps_production_source_29-01-2019']['establishments']
source_sectors = client['naps_production_source_29-01-2019']['sectors']

count = 0

for x in source_connection.find({}):

    found_course = name_mismatch.find_one({"_id": ObjectId(x['trade_id'])})

    if found_course is not None:
        found_establishment = source_establishment.find_one(
                {"_id": ObjectId(x["establishment_id"])})

        if x["sector_id"] != "GIix98Lu":
            found_sector = source_sectors.find_one({"_id": ObjectId(x["sector_id"])})
        x["trade_name"] = found_course["title"]
        x["establishment_name"] = found_establishment["name"]
        x["sector_name"] = found_sector["title"]
        x1 = target_collection.insert_one(x)

    count += 1
    print(count)

