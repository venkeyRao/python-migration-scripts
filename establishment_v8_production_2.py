import pymongo
from datetime import datetime
from bson.objectid import ObjectId
import sys


def mongo_connect(db, collection, connection='mongodb://localhost:27017'):
    connection = pymongo.MongoClient(connection)
    mongo_db = connection[db]
    mongo_collection = mongo_db[collection]

    return mongo_collection


# MongoDB source configuration for user collection
source_collection = mongo_connect('naps_production_source', 'e_locations')

# MongoDB target configuration for user collection
target_collection = mongo_connect('naps_production_target', 'establishments')

# MongoDB source configuration for candidates collection
source_collection1 = mongo_connect('naps_production_source', 'establishments')

connection = pymongo.MongoClient()

# Unique-code MongoDB
code_db = connection['naps_production_target']['_data_counters']

states_collection = mongo_connect('naps_production_target', 'states')

source_count = source_collection.count_documents({})

insert_count = 0

imported_array = []

state_dictionary = {}

state_identity = ""



for y in source_collection1.find({}):  # establishments
# for y in source_collection1.find(no_cursor_timeout=True).skip(1000).limit(1000): # establishments

    primary_id = y['_id']
    establishment_name = y['name'] if 'name' in y else ""
    registration_type = y['registration_type'] if 'registration_type' in y else ""
    registration_number = y['registration_number'] if 'registration_number' in y else ""
    type1 = y['type'] if 'type' in y else ""
    category = y['category'] if 'category' in y else ""
    industry_type = y['industry_type'] if 'industry_type' in y else ""
    working_days = y['working_days'] if 'working_days' in y else ""
    natural_resource = y['natural_resource'] if 'natural_resource' in y else ""
    strength = y['strength'] if 'strength' in y else ""
    updated_at = y['updated_at'] if 'updated_at' in y else ""
    created_at = y['created_at'] if 'created_at' in y else ""
    constitution_document = y['constitution_document'] if 'constitution_document' in y else ""
    gstin = y['gstin'] if 'gstin' in y else None
    is_gstin = True if 'gstin' in y else False
    no_gstin_reason = y['no_gstin_reason'] if 'no_gstin_reason' in y else ""
    pan = y['pan'] if 'pan' in y else ""
    permanent_emp_strength = y['permanent_emp_strength'] if 'permanent_emp_strength' in y else ""
    contractual_emp_strength = y['contractual_emp_strength'] if 'contractual_emp_strength' in y else ""
    state_count = y['state_count'] if 'state_count' in y else ""
    registration_number = y['registration_number'] if 'registration_number' in y else ""

    # print("--------------------------->contractual_emp_strength:<" + str(contractual_emp_strength) +
    #       ">    " + str(type(contractual_emp_strength)))

    array = []

    n = 0
    # e-locations
    for z in source_collection.find({'establishment_id': str(primary_id)}):
        location_name = z['name'] if 'name' in z else ""
        location_address_name = z['address'] if 'address' in z else ""
        locations_address_state_id = z['state_id'] if 'state_id' in z else ""  # Get state id
        locations_address_district_id = z['district_id'] if 'district_id' in z else ""  # get districe id
        locations_address_zipcode = z['zip_code'] if 'zip_code' in z else ""  # get zip code
        updated_at_arr = z['updated_at'] if 'updated_at' in z else ""
        created_at_arr = z['created_at'] if 'created_at' in z else ""

        if 'head' in z:
            head_name = z['head']['name'] if 'head' in z and 'name' in z['head'] else ""
            head_designation = z['head']['designation'] if "designation" in z['head'] else ""
            head_email = z['head']['email'] if "email" in z['head'] else ""
            head_updated_at = z['head']['updated_at'] if "updated_at" in z['head'] else ""
            head_created_at = z['head']['created_at'] if "created_at" in z['head'] else ""
            head_signature = z['head']['signature'] if "signature" in z['head'] else ""
            head_id = z['head']['_id'] if "_id" in z['head'] else ""

        else:
            head_name = ""
            head_designation = ""
            head_email = ""
            head_email = ""
            head_updated_at = ""
            head_signature = ""
            head_id = ""

        zzzid = z['_id']

        # global state_identity
        state_identity = locations_address_state_id

        array.append({
            "location_name": location_name,
            "code": "",
            "updated_at": updated_at_arr,
            "created_at": created_at_arr,
            "_id": zzzid,
            "deleted_at": "",
            "address": {
                "address_1": location_address_name,
                "address_2": "",
                "city": "",
                "state_id": locations_address_state_id,
                "zip_code": locations_address_zipcode,
                "district_id": locations_address_district_id,
                "updated_at": updated_at_arr,
                "created_at": created_at_arr,
                "_id": ObjectId(),
            },
        }
        )

        n += 1

        imported_array.append(z)

    import_batch = 1
    import_data_elocations = imported_array
    import_data_establishments = y
    import_data_establishments.pop("_id", None)

    # Code computation
    code = 'E'
    temp = created_at.timetuple() if not created_at == "" else ""
    code += str(temp[1]).zfill(2) if not created_at == "" else ""
    code += str(temp[0]) if not created_at == "" else ""

    

    if state_identity is not None:
        temp = states_collection.find_one({"_id": ObjectId(state_identity)})
        code += str(int(temp['code'])).zfill(2)

    code3 = code
    code_temp = code_db.find_one({"static": code3})

    if code_temp is None:
        z1 = code_db.insert_one(
            {
                "collection": "establishments",
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

    z = target_collection.insert_one(
        {
            "_id": primary_id,
            "is_ho": False,
            "registration_type": registration_type,
            "establishment_name": establishment_name,
            "landline_number": "",
            "registration_number": registration_number,
            "is_activated": True,
            "fields_not_updated": "",
            "strength": int(strength) if strength != '' and strength is not None else 0,
            "code": code2,
            "updated_at": updated_at,
            "created_at": created_at,
            "address": {
                "address_1": location_address_name,
                "address_2": "",
                "city": "",
                "state_id": locations_address_state_id,
                "zip_code": locations_address_zipcode,
                "district_id": locations_address_district_id,
                "updated_at": updated_at,
                "created_at": created_at,
                "_id": ObjectId(),
            },
            "category": category,
            "gstin": gstin,
            "industry_type": industry_type,
            "is_gstin": is_gstin,
            "no_gstin_reason": no_gstin_reason,
            "pan": pan,
            "type": type1,
            "head": {
                "updated_at": head_updated_at,
                "created_at": head_created_at,
                "_id": head_id,
                "name": head_name,
                "designation": head_designation,
                "email": head_email,
                "signature": head_signature
            },
            "contractual_emp_strength": int(contractual_emp_strength) if (contractual_emp_strength != '') and (contractual_emp_strength is not None) else 0,
            "natural_resource": natural_resource,
            "permanent_emp_strength": int(permanent_emp_strength) if permanent_emp_strength != '' and  permanent_emp_strength is not None else 0,
            "state_count": state_count,
            "working_days": working_days,
            "locations": array,
            "constitution_document": constitution_document,
            "import_batch": 1,
            "import_data": {
                "elocations": import_data_elocations,
                "establishments": import_data_establishments
            }
        })
    insert_count += 1
    print(insert_count)
    del array[:]
    del import_data_elocations[:]

if source_count == insert_count:
    print("\n\n All the documents were migrated successfully")
    print("\n<============================COMPLETED============================>")
    sys.exit(0)

else:
    print("\n\n Some data were not migrated!!!")
    print("\n<***************************INCOMPLETED***************************>")
    sys.exit(0)
