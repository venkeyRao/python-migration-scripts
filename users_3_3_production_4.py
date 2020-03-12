import pymongo
from datetime import datetime


def mongo_connect(db, collection, connection = 'mongodb://localhost:27017'):
    connection = pymongo.MongoClient(connection)
    mongo_db = connection[db]
    mongo_collection = mongo_db[collection]

    return mongo_collection


# MongoDB source configuration for user collection
source_connection = mongo_connect('naps_production_source', 'users')

# MongoDB target configuration for user collection
target_collection = mongo_connect('naps_production_target', 'users')

# MongoDB source configuration for candidates collection
source_collection1 = mongo_connect('naps_production_source', 'candidates')

# MongoDB establishment collection
source_establishment_collection = mongo_connect('naps_production_source', 'establishments')

source_count = source_connection.count_documents({})

insert_count = 0

connection = pymongo.MongoClient()

code_db = connection['naps_production_target']['_data_counters']

high_qualification_dictionary = {
    '1': '5c406444c8eeab700c2d5664',
    '2': '5c406444c8eeab700c2d5666',
    '3': '5c406444c8eeab700c2d566b',
    '4': '5c406444c8eeab700c2d5671',
    '5': '5c406444c8eeab700c2d5681'
}

candidate_category = {
    '1': 'general',
    '2': 'obc',
    '3': 'sc',
    '4': 'st'
}

for x in source_connection.find({}):

    primary_id = x['_id']
    role = x['role'] if 'role' in x else ""

    if role == 'e_admin':
        role = 'est_admin'
    
    status = x['status'] if 'status' in x else ""
    is_active = x['is_active'] if 'is_active' in x else ""
    activation_code = x['activation_code'] if 'activation_code' in x else ""
    email = x['email'] if 'email' in x else ""
    name = x['name'] if 'name' in x else ""
    password = x['password'] if 'password' in x else ""
    updated_at = x['updated_at'] if 'updated_at' in x else ""
    created_at = x['created_at'] if 'created_at' in x else ""

    candidates_target = {}

    # candidate_father_name= 
    """
    id proof
    father name
    technical qualifications
    candidate_id
    user id in source
    aadhar number
    """

    import_batch = 1
    import_data = x

    candidates = source_collection1.find_one( {'user_id': str(x['_id']) } )

    technical_qualification = []

    if candidates is None:
        candidates = ""

    else:

        candidate_id = candidates['_id']
        candidate_status = candidates['status'] if 'status' in candidates else ""
        candidates_is_active = candidates['is_active'] if 'is_active' in candidates else ""
        candidates_name = candidates['name'] if 'name' in candidates else ""
        candidate_date_of_birth = candidates['dob'] if 'dob' in candidates else ""
        candidate_gender = candidates['gender'] if 'gender' in candidates else ""
        candidate_father = candidates['father'] if 'father' in candidates else ""
        candidate_high_qualification = candidates['high_qualification_id'] if 'high_qualification_id' in candidates else ""
        qualification_type = high_qualification_dictionary[candidate_high_qualification] if candidate_high_qualification in high_qualification_dictionary else ""

        # Code computation
        code = 'A'
        temp = created_at.timetuple() if not created_at == "" else ""
        code += str(temp[1]).zfill(2) if not created_at == "" else ""
        code += str(temp[0]) if not created_at == "" else ""

        code3 = code
        # code_temp = code_db.find_one({code3: {"$exists": True}})
        code_temp = code_db.find_one({"static": code3})

        if code_temp is None:
            z1 = code_db.insert_one(
                {
                    "collection": "candidates",
                    "static": code,
                    "seq": 1
                    }
                )
            code2 = code + str('000001')

        else:
            code_temp = int(code_temp["seq"]) + 1
            code2 = str(code_temp).zfill(6)
            # code_db.update_many({code: {"$exists": True}}, {'$set': {code: code2}})
            code_db.update_many({"static": code3},
                                {'$set': {"seq": code_temp}})
            code2 = str(code) + str(code2)

        inserted_candidate_category = candidates['category'] if 'category' in candidates else ""

        if inserted_candidate_category == '':
            category = candidate_category[inserted_candidate_category]

        else:
            category = ''

        if candidate_high_qualification != '':

            candidates_target = {
                    "parent_name": candidate_father,
                    "parent_relation": "father",
                    "gender": candidate_gender,
                    "date_of_birth": candidate_date_of_birth,
                    "is_activated": True,
                    "fields_not_updated": "",
                    "code": code2,
                    "updated_at": candidates['updated_at'] if 'updated_at' in candidates else "",
                    "created_at": candidates['created_at'] if 'created_at' in candidates else "",
                    "_id": candidate_id,
                    "category": category,
                    "dob_file": candidates['dob_photo'] if 'dob' in candidates else "",
                    "guardian_name": "",
                    "guardian_relation": "",
                    "id_proof": "",
                    "id_proof_number": candidates['aadhar_no'] if 'aadhar_no' in candidates else "",
                    "id_proof_type": "aadhar",
                    "is_disable": candidates['disability'] if 'disability' in candidates else "",
                    "pmkvy_no": "",
                    "profile_image": candidates['photo'] if 'photos' in candidates else "",
                    "signature_image": candidates['signature'] if 'signature' in candidates else "",
                    "address": {
                        "address_1": candidates['address']['address'] if 'address' in candidates['address'] else "",
                        "address_2": "",
                        "city": "",
                        "state_id": candidates['address']['state_id'] if 'state_id' in candidates['address'] else "",
                        "district_id": candidates['address']['district_id'] if 'district_id' in candidates['address'] else "",
                        "zip_code": candidates['address']['pincode'] if 'pincode' in candidates['address'] else "",
                        "updated_at": candidates['address']['updated_at'] if 'updated_at' in candidates['address'] else "",
                        "created_at": candidates['address']['created_at'] if 'created_at' in candidates['address'] else "",
                        "_id": candidates['address']['_id'] if '_id' in candidates['address'] else ""
                    },
                    "candidateQualifications": [{
                            "is_highest": True,
                            "institute": "",
                            "aggregate_marks": "",
                            "total_marks": "",
                            "percentage": "",
                            "start_date": "",
                            "end_date": "",
                            "qualification_document": candidates['qualification_document'] if 'qualification_document' in candidates else "",
                            "updated_at": "",
                            "created_at": "",
                            "_id": "",
                            "qualification": {
                                "qualification_type": qualification_type,
                                "qualification": "",
                                "specialization": "",
                                "updated_at": "",
                                "created_at": "",
                                "_id": ""
                            }
                        }]
                }

        else:
            candidates_target = {
                    "parent_name": candidate_father,
                    "parent_relation": "father",
                    "gender": candidate_gender,
                    "date_of_birth": candidate_date_of_birth,
                    "is_activated": True,
                    "fields_not_updated": "",
                    "code": code2,
                    "updated_at": candidates['updated_at'] if 'updated_at' in candidates else "",
                    "created_at": candidates['created_at'] if 'created_at' in candidates else "",
                    "_id": candidate_id,
                    "category": category,
                    "dob_file": candidates['dob_photo'] if 'dob' in candidates else "",
                    "guardian_name": "",
                    "guardian_relation": "",
                    "id_proof": "",
                    "id_proof_number": candidates['aadhar_no'] if 'aadhar_no' in candidates else "",
                    "id_proof_type": "aadhar",
                    "is_disable": candidates['disability'] if 'disability' in candidates else "",
                    "pmkvy_no": "",
                    "profile_image": candidates['photo'] if 'photos' in candidates else "",
                    "signature_image": candidates['signature'] if 'signature' in candidates else "",
                    "address": {
                        "address_1": candidates['address']['address'] if 'address' in candidates['address'] else "",
                        "address_2": "",
                        "city": "",
                        "state_id": candidates['address']['state_id'] if 'state_id' in candidates['address'] else "",
                        "district_id": candidates['address']['district_id'] if 'district_id' in candidates['address'] else "",
                        "zip_code": candidates['address']['pincode'] if 'pincode' in candidates['address'] else "",
                        "updated_at": candidates['address']['updated_at'] if 'updated_at' in candidates['address'] else "",
                        "created_at": candidates['address']['created_at'] if 'created_at' in candidates['address'] else "",
                        "_id": candidates['address']['_id'] if '_id' in candidates['address'] else ""
                    }
                }


    establishment_link = source_establishment_collection.find_one({"user_id": str(primary_id)})

    if establishment_link is not None and role == 'est_admin':

        print("not mapping with establishment: " + str(primary_id))

        z = target_collection.insert_one(
            {
                "_id": primary_id,
                "status": status,
                "is_active": is_active,
                "activation_code": activation_code,
                "email": email,
                "name": name,
                "role": role,
                "mobile": "",
                "password": password,
                "updated_at": updated_at,
                "created_at": created_at,
                "candidate": candidates_target,
                "establishment_id": str(establishment_link['_id']),
                "import_batch": import_batch,
                "imported_data": {
                    "user": x,
                    "candidate": candidates
                }
            }
        )

    else:
        z = target_collection.insert_one(
            {
                "_id": primary_id,
                "status": status,
                "is_active": is_active,
                "activation_code": activation_code,
                "email": email,
                "name": name,
                "role": role,
                "mobile": "",
                "password": password,
                "updated_at": updated_at,
                "created_at": created_at,
                "candidate": candidates_target,
                "import_batch": import_batch,
                "imported_data": {
                    "user": x,
                    "candidate": candidates
                }
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
