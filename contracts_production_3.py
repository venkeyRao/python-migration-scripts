import pymongo
from datetime import datetime
from bson.objectid import ObjectId
from pprint import pprint
import sys


def mongo_connect(db, collection, connection='mongodb://localhost:27017'):
    connection = pymongo.MongoClient(connection)
    mongo_db = connection[db]
    mongo_collection = mongo_db[collection]

    return mongo_collection


def offer_date_strip(temp_date):
    try:
        temp_date2 = temp_date.split('-')

        offer_date_temp1 = temp_date2[0]  # year
        offer_date_temp2 = temp_date2[1]  # month
        offer_date_temp3 = temp_date2[2]  # day

        temp_date3 = offer_date_temp3 + '-' + offer_date_temp2 + '-' + offer_date_temp1

        return temp_date3

    except IndexError:
        # print("<======================>")
        # print("\n\n")
        # print("temp_date: " + str(temp_date))

        if '/' in temp_date:
            temp_date2 = temp_date.split('/')

            offer_date_temp1 = temp_date2[0]  # year
            offer_date_temp2 = temp_date2[1]  # month
            offer_date_temp3 = temp_date2[2]  # day

        temp_date3 = offer_date_temp3 + '-' + offer_date_temp2 + '-' + offer_date_temp1

        return temp_date3


def find_daye_difference(daye_string1, daye_string2):
    daye_temp_date1 = daye_string1.split('-')

    daye_date_temp1 = daye_temp_date1[0]  # day
    daye_date_temp2 = daye_temp_date1[1]  # month
    daye_date_temp3 = daye_temp_date1[2]  # year

    try:
        daye_string1_complete = datetime(int(daye_date_temp3), int(daye_date_temp2), int(daye_date_temp1))

    except ValueError:
        daye_string1_complete = datetime(int(daye_date_temp3), int(daye_date_temp1), int(daye_date_temp2))

    daye_temp_date2 = daye_string2.split('-')

    daye_date_temp11 = daye_temp_date2[0]  # day
    daye_date_temp22 = daye_temp_date2[1]  # month
    daye_date_temp33 = daye_temp_date2[2]  # year

    try:
        daye_string2_complete = datetime(int(daye_date_temp33), int(daye_date_temp22), int(daye_date_temp11))

    except ValueError:
        daye_string2_complete = datetime(int(daye_date_temp33), int(daye_date_temp11), int(daye_date_temp22))

    their_date_of_birth = (daye_string1_complete - daye_string2_complete).days

    year = their_date_of_birth // 365
    month = (their_date_of_birth % 365) // 30
    day = (their_date_of_birth % 365) % 30

    result_string = str(year) + ' years, ' + str(month) + ' months and ' + str(day) + ' days'
    return result_string


def dob_generator(temp_date_of_birth):
    try:
        # temp_date_of_birth1 = temp_date_of_birth.split('-')
        temp_date_of_birth1 = temp_date_of_birth.split('-')

        temp_dob_datetime = datetime(
            int(temp_date_of_birth1[2]), int(temp_date_of_birth1[1]), int(temp_date_of_birth1[0]))

        current_date = datetime.now()

        actual_dob = (current_date - temp_dob_datetime).days  # in days

        dob_year = actual_dob // 365
        dob_month = (actual_dob % 365) // 30
        dob_day = (actual_dob % 365) % 30

        dob_string = str(dob_year) + ' years, ' + str(dob_month) + ' months and ' + str(dob_day) + ' days'
        return dob_string

    except ValueError:
        temp_date_of_birth1 = temp_date_of_birth.split('-')

        temp_dob_datetime = datetime(
            int(temp_date_of_birth1[2]), int(temp_date_of_birth1[0]), int(temp_date_of_birth1[1]))

        current_date = datetime.now()

        actual_dob = (current_date - temp_dob_datetime).days  # in days

        dob_year = actual_dob // 365
        dob_month = (actual_dob % 365) // 30
        dob_day = (actual_dob % 365) % 30

        dob_string = str(dob_year) + ' years, ' + str(dob_month) + \
                     ' months and ' + str(dob_day) + ' days'
        return dob_string


applications_id = ["5c08f3d1a31ef80f1c1be682",
                   "5c0a5647d9233122834d6d02",
                   "5c0a5664d9233122aa0e1592",
                   "5c0b974ca31ef807c03ff512",
                   "5c0e4938d9233122a274a112",
                   "5c0e6602d9233136082d5962",
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
target_collection = mongo_connect('temp', 'contracts_chef_3')

# Target Applications
target_applications_collection = mongo_connect('temp', 'applications_chef')

target_establishment_collection = mongo_connect('naps_production_29012019', 'establishments')

target_users_collection = mongo_connect('naps_production_29012019', 'users')

target_oppertunities_collection = mongo_connect('naps_source', 'opportunities')

target_establishments_collection = mongo_connect(
    'naps_source', 'establishments')

target_candidates_collection = mongo_connect('naps_source', 'candidates')

connection = pymongo.MongoClient()

# Unique-code MongoDB
code_db = connection['naps_production_target']['_data_counters']

# MongoDB source configuration for candidates collection
# source_collection1 = mongo_connect('naps_source_dump_jan3', 'applications')

migrated_users_collection = mongo_connect('naps_production_target', 'users')

source_count = source_connection.count_documents({})

insert_count = 0

for new in applications_id:
    # for x in source_connection.find({"apprenticeship": {"$exists": True}}):
    x = source_connection.find_one({"_id": ObjectId(new)})

    if 'apprenticeship' in x:

        print("x: " + str(x["_id"]))
        print(x['_id'])

        target_applications = target_applications_collection.find_one({"_id": ObjectId(x['_id'])})
        # print("target_application: " + str(target_applications))
        # print(f"application id: {str(target_applications['_id'])}")

        if target_applications is not None:

            # target_application_oppertunity_id = target_applications['opportunity_id']

            target_oppertunities = target_oppertunities_collection.find_one(
                {"_id": ObjectId("5c4eaed844f7d776af09351d")})

            # master_temp = migrated_oppertunities.find_one(
            #     {"_id": ObjectId(x['course_id'])})

            temp_id = x['course_id']
            print("1")

            temp_id = "5c4166ecc8eeab06dd325703"

            master_temp = target_oppertunities_collection.find_one(
                {"_id": ObjectId("5c4166ecc8eeab06dd325703")})

            # if master_temp is not None:
            if True:

                establishment_collection_establishment_id = x['establishment_id']

                target_establishment = target_establishment_collection.find_one(
                    {"_id": ObjectId("5b97af50a31ef821bc6950c3")})

                target_application_candidate_id = target_applications['candidate_id']

                target_users = target_users_collection.find_one(
                    {"_id": ObjectId(target_application_candidate_id)})

                temp_location_id = target_applications['location_id']

                target_establishment_establishment_id = x['establishment_id']

                target_aaa = target_establishment_collection.find_one(
                    {"_id": ObjectId("5b97af50a31ef821bc6950c3")})

                onJobTrainingLocation = None

                print("2")

                if target_aaa != None:

                    for locations_count, target_bbb in enumerate(target_aaa['locations']):
                        # pprint("\n\n\ntarget_bbb: " + str(target_bbb))

                        if str(target_bbb['_id']) == temp_location_id:
                            onJobTrainingLocation = target_bbb
                            break

                # candidate_location_id = target_establishments_collection.find_one({""})

                print("3")

                on_job_training_period_from = offer_date_strip(x['offer']['start_date'])
                on_job_training_period_to = offer_date_strip(x['offer']['end_date'])

                apprenticeship_training_duration = find_daye_difference(
                    on_job_training_period_to, on_job_training_period_from)

                temp_status = x['apprenticeship']['status']

                created_at_temp = x['apprenticeship']['created_at']
                created_at_temp = created_at_temp.timetuple()
                code_month_year = str(created_at_temp[1]).zfill(2)
                code_month_year += str(created_at_temp[0])

                if temp_status == 'pending':
                    is_valid = False
                    is_active = True
                    can_sign = True
                    can_reissue = False
                    status = temp_status
                    code_abbrevation = 'TMPCON'

                elif temp_status == 'cand_signed':
                    is_valid = True
                    is_active = True
                    can_sign = False
                    can_reissue = False
                    status = temp_status
                    code_abbrevation = 'CON'

                elif temp_status == 'approved':
                    is_valid = True
                    is_active = True
                    can_sign = False
                    can_reissue = False
                    status = 'verified'
                    code_abbrevation = 'CON'

                elif temp_status == 'rejected':
                    is_valid = False
                    is_active = False
                    can_sign = False
                    can_reissue = False
                    status = temp_status
                    code_abbrevation = 'CON'

                elif temp_status == 'completed':
                    is_valid = True
                    is_active = False
                    can_sign = False
                    can_reissue = False
                    status = temp_status
                    code_abbrevation = 'CON'

                elif temp_status == 'est_signed':
                    is_valid = False
                    is_active = True
                    can_sign = True
                    can_reissue = False
                    status = 'pending'
                    code_abbrevation = 'CON'

                code_prefix = code_abbrevation + code_month_year

                code_temp = code_db.find_one({"static": code_prefix})

                if code_temp is None:
                    z1 = code_db.insert_one(
                        {
                            "collection": "contracts",
                            "static": code_prefix,
                            "seq": 1
                        }
                    )
                    code2 = code_prefix + str('000001')

                else:
                    code_temp = int(code_temp["seq"]) + 1
                    code2 = str(code_temp).zfill(6)
                    code_db.update_many({"static": code_prefix},
                                        {'$set': {"seq": code_temp}})
                    code2 = str(code_prefix) + str(code2)

                print("found candidate id:")
                print(str(target_applications['candidate_id']))

                target_users = target_users_collection.find_one({"_id": ObjectId(target_applications['candidate_id'])})

                # target_users = target_users['user_id']
                print("target user id:")
                pprint(target_users)
                #
                # target_users = target_users_collection.find_one({"_id": ObjectId(target_users)})

                temp_dob = target_users['candidate']['date_of_birth']

                target_applications['course_id'] = "5c4166ecc8eeab06dd325703"

                abc = {
                    "qualification": {
                        "qualification": None,
                        "qualification_type": "5c406444c8eeab700c2d5664",
                        "_id": ObjectId(),
                        "specialization": "",
                        "updated_at": datetime.now(),
                        "created_at": datetime.now()
                    },
                    "start_date": "",
                    "aggregate_marks": "",
                    "total_marks": "",
                    "end_date": "",
                    "created_at": datetime.now(),
                    "institute": "",
                    "percentage": "",
                    "is_highest": True,
                    "qualification_document": "documents/candidate/education/academic_doc/TKbn2kqG1l8c6MdBxa9sXJemLQgsrP2nyDRGLa2blOW8Wju7yv0kwOxGYReT.jpg",
                    "_id": ObjectId(),
                    "updated_at": datetime.now()
                }

                try:
                    if target_users['candidate']['candidate'] is None:
                        print("yes")

                except KeyError:
                    target_users['candidate'] = {}
                    # target_users['candidate']['candidate'] = {}
                    target_users['candidate']['candidateQualifications'] = []
                    target_users['candidate']['candidateQualifications'].append(abc)

                try:
                    target_users['candidate']['candidateQualifications'][0]['qualification']['qualification'] = None
                    target_users['candidate']['candidateQualifications'][0]['qualification'][
                        'updated_at'] = datetime.now()
                    target_users['candidate']['candidateQualifications'][0]['qualification'][
                        'created_at'] = datetime.now()

                except KeyError:
                    target_users['candidate']['candidate']['candidateQualifications'][0]['qualification'][
                        'qualification'] = None
                    target_users['candidate']['candidate']['candidateQualifications'][0]['qualification'][
                        'updated_at'] = datetime.now()
                    target_users['candidate']['candidate']['candidateQualifications'][0]['qualification'][
                        'created_at'] = datetime.now()

                z = target_collection.insert_one(
                    {
                        "_id": ObjectId(),
                        "status": status,
                        "is_valid": is_valid,
                        "is_active": is_active,
                        "can_sign": can_sign,
                        "can_reissue": can_reissue,
                        "application_id": x['_id'],
                        "application": target_applications,
                        "establishment_id": x['establishment_id'],
                        "establishment": target_establishment,
                        "candidate_id": target_applications['candidate_id'],
                        "candidate": target_users,
                        "opportunity_id": "5c4eaed844f7d776af09351d",
                        "opportunity": target_oppertunities,
                        "code": code2,
                        "updated_at": x['apprenticeship']['updated_at'],
                        "created_at": x['apprenticeship']['created_at'],
                        "candidateHighestQualification": target_users['candidate']['candidateQualifications'][
                            0] if 'candidateQualifications' in target_users else None,
                        "bt_exemption_reason": target_users['candidate']['candidateQualifications'][
                            0] if 'candidateQualifications' in target_users else None,
                        "age_of_candidate": dob_generator(temp_dob),
                        "apprenticeship_training_duration": apprenticeship_training_duration,
                        "basic_training_applicable": False,
                        "btc_id": None,
                        "btp_id": None,
                        "is_naps": True,
                        "onJobTrainingLocation": onJobTrainingLocation,
                        "stipend_first": x['offer']['stipend_amount']['first'],
                        "stipend_second": x['offer']['stipend_amount']['second'],
                        "stipend_third": x['offer']['stipend_amount']['third'],
                        "tpa_applicable": "no",
                        "tpa_name": None,
                        "trainingPeriods": [{
                            "on_job_training_period_from": offer_date_strip(x['offer']['start_date']),
                            "on_job_training_period_to": offer_date_strip(x['offer']['end_date']),
                            "_id": ObjectId()
                        }],
                        "contract_document": x['apprenticeship']['contract'],
                        "import_batch": 4,
                        "import_data": x
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
