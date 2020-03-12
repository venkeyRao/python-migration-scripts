from pymongo import MongoClient

connection = MongoClient()

users_collection = connection['naps_production_target']['users']

import_batch_1_count = 0

slash_count = 0

dash_count = 0

run_count = 0

for x in users_collection.find({"role": 'candidate'}):

    temp_dob = x['candidate']['date_of_birth']

    if x['imported_data']['candidate']['import_batch'] == 1:
        changed_dob = temp_dob.replace('/', '-')
        import_batch_1_count += 1

    elif '/' in temp_dob:
        # temp_dob = temp_dob.replace('/', '-')

        temp_dob = temp_dob.split('/')
        changed_dob = temp_dob[1] + '-' + temp_dob[0] + '-' + temp_dob[2]

        slash_count += 1

    elif '-' in temp_dob:
        temp_dob = temp_dob.split('-')
        changed_dob = temp_dob[2] + '-' + temp_dob[1] + '-' + temp_dob[0]

        dash_count += 1

    x1 = users_collection.find_one_and_update(
        {"_id": x["_id"]}, {"$set": {"candidate.date_of_birth": changed_dob}} )

    run_count += 1
    print(str(run_count) + '/1193824')

print("import_batch_1_count: " + str(import_batch_1_count))
print("slash_count: " + str(slash_count))
print("dash_count: " + str(dash_count))
