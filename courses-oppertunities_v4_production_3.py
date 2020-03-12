import pymongo
from datetime import datetime
from bson.objectid import ObjectId


def mongo_connect(db, collection, connection='mongodb://localhost:27017'):
    connection = pymongo.MongoClient(connection)
    mongo_db = connection[db]
    mongo_collection = mongo_db[collection]

    return mongo_collection


# MongoDB source configuration for user collection
source_connection = mongo_connect('naps_production_source', 'courses')

# MongoDB target configuration for user collection
target_collection = mongo_connect('temp', 'opportunities_missmatch')

# Unique-code MongoDB
trade_collection = mongo_connect('naps_production_source', 'trades')

# Target Establishment
target_establishment = mongo_connect(
    'naps_production_target', 'establishments')

# preprod course connection
preprod_course = mongo_connect('naps_preprod', 'courses_2')

course_locations = mongo_connect('naps_production_source', 'course_locations')

connection = pymongo.MongoClient()

# Unique-code MongoDB
code_db = connection['naps_production_target']['_data_counters']

source_count = source_connection.count_documents({})

mismatching_ids = mongo_connect('naps_production_target', 'courses_mismatching_ids')

insert_count = 0

none_count = 0

course_insert_count = 0

missing_ids = []

for x in source_connection.find({}):



    primary_id = x['_id']
    # courses_id = x['trade_id']
    status = x['status']
    sector_id = x['sector_id']
    establishment_id = x['establishment_id']
    updates_at = x['updated_at']
    created_at = x['created_at']

    import_batch = 1
    import_data = x

    if status == 'enabled':
        status = True

    else:
        status = False

    y = trade_collection.find_one({"_id": ObjectId(x['trade_id'])})

    # print("y: " + str(y))

    if y is not None:
        new_course = preprod_course.find_one({"name": y['title']})

    else:
        new_course = None

    # print("new course: " + str(new_course))

    if new_course is None:
        none_count += 1
        course_id = ""

    else:
        miss = mismatching_ids.insert_one({"id": str(primary_id)})

    # else:
    #     if y is not None:
    #         courses_id = new_course['_id']
    #         course_insert_count += 1

    if y is not None:
        if new_course is not None:
            courses_id = new_course['_id']
            course_insert_count += 1

        else:
            courses_id = ""

    if y is None:
        y = {}

    # Code computation
    code = 'AO'
    # temp = created_at.timetuple() if not created_at == "" else ""
    # code += str(temp[1]).zfill(2) if not created_at == "" else ""
    # code += str(temp[0]) if not created_at == "" else ""

    code_temp = code_db.find_one({"static": 'AO'})

    if code_temp is None:
        z1 = code_db.insert_one(
            {
                "collection": "opportunities",
                "static": code,
                "seq": 1
            }
        )
        code2 = code + str('000001')

    else:
        code_temp = int(code_temp["seq"]) + 1
        code2 = str(code_temp).zfill(6)
        code_db.update_many({"static": 'AO'},
                            {'$set': {"seq": code_temp}})
        code2 = str(code) + str(code2)

    number_of_vacancies = 0
    available_vacancies = 0

    for locations in course_locations.find({"course_id": str(primary_id)}):

        if locations is not None:

            if 'available_seats' in locations:
                available_vacancies += locations['available_seats']

            if 'openings' in locations:
                number_of_vacancies += locations['openings']

    establishment_data = target_establishment.find_one({"_id": ObjectId(establishment_id)})

    if establishment_data is None:
        establishment_data = ""
        
    if new_course is not None:
        z = target_collection.insert_one(
            {
                "_id": primary_id,
                "status": status,
                "name": new_course['name'],
                "number_of_vacancies": number_of_vacancies,
                "available_vacancies": available_vacancies,
                "gender_type": "",
                "short_description": "",
                "stipend_from": "",
                "stipend_upto": "",
                "naps_benefit": True,
                "course_id": courses_id,
                "course": new_course,
                "trainings": new_course['trainings'] if 'trainings' in new_course else [],
                "establishment_id": establishment_id,
                "establishment": establishment_data,
                "code": code2,
                "created_by": "",
                "updated_at": updates_at,
                "created_at": created_at,
                "document": "",
                "updated_by": "",
                "import_batch": 1,
                "import_data": {
                    "courses" : x,
                    "trades" : y
                }
            }
        )
        insert_count += 1
        print(insert_count)
        print("id count: " + str(course_insert_count))

        print("none count: " + str(none_count))

    elif y['type'] == 'optional':
        missing_ids.append(y['title'])


print("\n\n\n")
print(missing_ids)
print("\n\n\n")

print("Mis Match Count :" + str(len(missing_ids)))

if source_count == insert_count:
    print("\n\n All the documents were migrated successfully")
    print("\n<============================COMPLETED============================>")

else:
    print("\n\n Some data were not migrated!!!")
    print("\n<***************************INCOMPLETED***************************>")
