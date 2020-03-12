import pymongo
from datetime import datetime
from bson.objectid import ObjectId


def mongo_connect(db, collection, connection='mongodb://localhost:27017'):
    connection = pymongo.MongoClient(connection)
    mongo_db = connection[db]
    mongo_collection = mongo_db[collection]

    return mongo_collection


applications_array = [  # "5c08f3d1a31ef80f1c1be682",
    #                       "5c0a5647d9233122834d6d02",
    #                       "5c0a5664d9233122aa0e1592",
    #                       "5c0b974ca31ef807c03ff512",
    #                       "5c0e4938d9233122a274a112",
    #                       "5c0e6602d9233136082d5962",
    "5b9a1840a31ef85a07329002",
    "5b9a1f42a31ef85ebe75e7c2",
    "5bec0e1fd923315bbb2d1a52",
    # "5c08f3d1a31ef80f1c1be682",
    # "5c0a5647d9233122834d6d02",
    # "5c0a5664d9233122aa0e1592",
    # "5c0b974ca31ef807c03ff512",
    # "5c0e4938d9233122a274a112",
    # "5c0e6602d9233136082d5962",
    "5c398d9fa31ef81b6210d052",
    "5c3dab8bd9233164350be4c2",
    "5c3db34dd92331652e5b8482",
    "5c3dc8cad9233167c94c34f2",
    "5c3dcadaa31ef86638147d82",
    "5c3ecd47d923317a66404092",
    "5c3eda85a31ef8383a03c8d2",
    "5c3efffda31ef846e77bbcb2",
    "5c3f0074d9233104ad28dfa2",
    "5c3f00f8a31ef84740588632",
    "5c3f0192d9233104f2568f78",
    "5c400925a31ef8166a02dfb2",
    "5c400c0dd9233118ff2d4b02"
]

# MongoDB source configuration for user collection
source_connection = mongo_connect('naps_source', 'applications')

# MongoDB target configuration for user collection
target_collection = mongo_connect('temp', 'applications_chef_2')

connection = pymongo.MongoClient()

# Unique-code MongoDB
code_db = connection['naps_production_target']['_data_counters']

# MongoDB source configuration for candidates collection
# source_collection1 = mongo_connect('naps_source_dump_jan3', 'applications')

migrated_users_collection = mongo_connect('naps_production_29012019', 'users')

# Migrated source collection in the production source dump
course_locations = mongo_connect('naps_source', 'course_locations')

migrated_oppertunities = mongo_connect(
    'naps_production_29012019', 'oppertunities')

source_count = source_connection.count_documents({})

insert_count = 0

i_dont_know_count = 0

for new in applications_array:

    x = source_connection.find_one({"_id": ObjectId(new)})

    primary_id = x['_id']
    oppertunity_id = x['course_id'] if 'course_id' in x else ""
    candidate_id = x['candidate_id'] if 'candidate_id' in x else ""
    establishment_id = x['establishment_id'] if 'establishment_id' in x else ""
    updated_at = x['updated_at'] if 'updated_at' in x else None
    created_at = x['created_at'] if 'created_at' in x else None
    location_id = x['location_id'] if 'location_id' in x else ""

    import_batch = 1
    import_data = x

    # Code computation
    code = 'AP'
    temp = created_at.timetuple()
    code += str(temp[1]).zfill(2)
    code += str(temp[0])

    code3 = code
    code_temp = code_db.find_one({"static": code3})

    if code_temp is None:
        z1 = code_db.insert_one(
            {
                "collection": "applications",
                "static": code,
                "seq": 1
            }
        )
        code2 = code + str('000001')

    else:
        code_temp = int(code_temp["seq"]) + 1
        code2 = str(code_temp).zfill(6)
        code_db.update_many({"static": code3},
                            {'$set': {"seq": code_temp}})
        code2 = str(code) + str(code2)

    # print("---------------->STATUS: " +
    #       str(x['status']) + "-----------" + str(x['offer']['status']) + "------------" + str(x['apprenticeship']['status']))

    # print(">>>>>>>>>>>>>>>>>>>>>>status" + x['status'])

    # print(">>>>>>>>>>>>>>>>>>>>>>>>>>  _id:  " + str(primary_id))

    if x['status'] == 'pending':
        status = 'pending'

    elif x['status'] == 'rejected':
        status = 'rejected'

    elif x['status'] == 'offered':

        if x['offer']['status'] == 'pending':
            status = 'pending'

        elif x['offer']['status'] == 'rejected':
            status = 'rejected'

        elif x['offer']['status'] == 'accepted':

            # if x['apprenticeship']['status'] == 'pending':
            #     status = 'issue_contract'

            # elif x['apprenticeship']['status'] == 'accepted':
            #     status = 'issue_contract'

            # elif x['apprenticeship']['status'] == 'accepted':
            #     status = 'issue_contract'

            # elif 

            if 'apprenticeship' in x:
                status = 'issue_contract'

            else:
                status = 'pending'

    found_user = migrated_users_collection.find_one(
        {"candidate._id": ObjectId(candidate_id)})

    if found_user is None:
        print("<<<<<<<<<<<NONE======Candidateid>:::" + candidate_id)

    candidate_id = str(found_user['_id'])

    new_location = course_locations.find_one({"_id": ObjectId(location_id)})

    # print("ID: " + str(location_id))

    new_location_id = new_location['location_id'] if new_location is not None else None

    master_temp = migrated_oppertunities.find_one({"_id": ObjectId(x['course_id'])})

    # if master_temp != None:
    if True:
        z = target_collection.insert_one(
            {
                "_id": primary_id,
                "opportunity_id": oppertunity_id,
                "establishment_id": establishment_id,
                "candidate_id": candidate_id,
                "location_id": new_location_id,
                "code": code2,
                "updated_at": updated_at,
                "created_at": created_at,
                "approvals": [
                    {
                        "status": status,
                        "updated_at": None,
                        "created_at": None,
                        "_id": ObjectId()
                    }
                ],
                "import_batch": 4,
                "import_data": x
            }
        )
        insert_count += 1
        print(insert_count)

    i_dont_know_count += 1
    print("i dont know: " + str(i_dont_know_count))

if source_count == insert_count:
    print("\n\n All the documents were migrated successfully")
    print("\n<============================COMPLETED============================>")

else:
    print("\n\n Some data were not migrated!!!")
    print("\n<***************************INCOMPLETED***************************>")
