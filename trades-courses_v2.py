import pymongo
from datetime import datetime


def mongo_connect(db, collection, connection='mongodb://localhost:27017'):
    connection = pymongo.MongoClient(connection)
    mongo_db = connection[db]
    mongo_collection = mongo_db[collection]

    return mongo_collection


# MongoDB source configuration for user collection
source_connection = mongo_connect('naps_source_dump_jan3', 'trades')

# MongoDB target configuration for user collection
target_collection = mongo_connect('apptp_migrated_dump_jan9', 'courses')

source_count = source_connection.count_documents({})

insert_count = 0

for x in source_connection.find({}):

    primary_id = x['_id']
    name = x['title'] if "title" in x else ""
    course_type = x['type'] if "type" in x else ""
    bt_duration = x['duration'] if "duration" in x else ""
    sector_id = x['sector_id'] if "sector_id" in x else ""
    updates_at = x['updated_at'] if "updated_at" in x else ""
    created_at = x['created_at'] if "created_at" in x else ""
    curriculum = x['curriculum'] if "curriculum" in x else ""
    trainings_id = x['tid'] if "tid" in x else ""

    import_batch = 1
    import_data = x

    z = target_collection.insert_one(
        {
            "_id": primary_id,
            "status": "approved",
            "name": name,
            "description": "",
            "nsqf_level": "",
            "sector_id": sector_id,
            "is_naps": True,
            "created_by": "",
            "qps": [
                {
                    "_id": "",
                    "status": "",
                    "name": "",
                    "code": "",
                    "description": "",
                    "nsqf_level": "",
                    "sector_id": "",
                    "updated_at": "",
                    "created_at": "",
                    "curriculum": curriculum
                }
            ],
            "code": "",
            "updated_at": updates_at,
            "created_at": created_at,
            "owner_id": "",
            "owner_type": "",
            "type": course_type,
            "qualification": {
                "qualification_type": "",
                "qualification": "",
                "specialization": "",
                "updated_at": "",
                "created_at": "",
                "_id": ""
            },
            "trainings": [
                {
                    "bt_duration": bt_duration,
                    "ojt_duration": bt_duration,
                    "updated_at": "",
                    "created_at": "",
                    "_id": trainings_id
                }
            ],
            "curriculum": "",
            "approvals": [
                {
                    "status": "",
                    "updated_at": "",
                    "created_at": "",
                    "_id": "",
                    "updated_by": "",
                    "remarks": ""
                }
            ],
            "is_enabled": "",
            "import_batch": 1,
            "imported_data": x
        }
    )
    insert_count += 1
    print(insert_count)

if source_count == insert_count:
    print("\n\n All the documents were migrated successfully")
    print("\n<============================COMPLETED============================>")

else:
    print("\n\n Some data were not migrated!!!")
    print("\n<***************************INCOMPLETED***************************>")
