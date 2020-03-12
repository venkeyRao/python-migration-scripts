from pymongo import MongoClient
from bson.objectid import ObjectId

client = MongoClient()

source = client['naps_production_source_29-01-2019']['applications']

target = client['mismatch']['applications_mismatch_apprentice_complete_2']

mismatch_courses = client['mismatch']['mismatch_courses']

source_candidates = client['naps_production_source_29-01-2019']['candidates']

source_establishment = client['naps_production_source_29-01-2019']['establishments']

source_user = client['naps_production_source_29-01-2019']['users']

count = 0

for x in source.find({}):

    # print(x)
    obj = ObjectId(x['course_id'])
    found_course = mismatch_courses.find_one({"_id": obj})

    if found_course is not None:
        if 'apprenticeship' in x:
            found_candidate = source_candidates.find_one({"_id": ObjectId(x["candidate_id"])})
            found_candidate_user = found_candidate["user_id"]
            found_user = source_user.find_one({"_id":ObjectId(found_candidate_user)})
            found_user_name = found_user["name"]
            found_establishment = source_establishment.find_one({"_id": ObjectId(x["establishment_id"])})

            x["candidate_name"] = found_candidate["name"] if 'name' in found_candidate else ''
            x["establishment_name"] = found_establishment["name"]
            x["user_name"] = found_user_name
            x1 = target.insert_one(x)

    count += 1

    print(count)

