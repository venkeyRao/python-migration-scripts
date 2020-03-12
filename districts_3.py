import pymongo
from bson.objectid import ObjectId


def mongo_connect(db, collection, connection='mongodb://localhost:27017'):
    connection = pymongo.MongoClient(connection)
    mongo_db = connection[db]
    mongo_collection = mongo_db[collection]

    return mongo_collection

missing_dictionary = {
    "Ahmedabad": "Ahmadabad",
    "Angul": "Anugul",
    "Aravalli": "Arvalli",
    "Banaskantha": "Banas Kantha",
    "Bangalore Rural": "Bengaluru Rural",
    "Bellary": "Ballari",
    "Bishwanath": "Biswanath",
    "Budgam": "Badgam",
    "Central Delhi": "Central",
    "Charkhi Dadri": "Charki Dadri",
    "Chhota Udaipur": "Chhotaudepur",
    "Chikkaballapur": "Chikballapur",
    "Cooch Behar": "Coochbehar",
    "Dakshina Kannada": "Dakshin Kannad",
    "Dantewada (south Bastar)": "Dantewada",
    "Davanagere": "Davangere",
    "Devbhoomi Dwarka": "Devbhumi Dwarka",
    "Dahod": "Dohad",
    "East Singhbhum": "East Singhbum",
    "Firozpur": "Firozepur",
    "Gariaband": "Gariyaband",
    "Gautam Budh Nagar": "Gautam Buddha Nagar",
    "Jagatsinghpur": "Jagatsinghapur",
    "Jagtial": "Jagitial",
    "Jajpur": "Jajapur",
    "Jangaon": "Jangoan",
    "Kaimur": "Kaimur (bhabua)",
    "Kamrup Metropolitan": "Kamrup Metro",
    "Kanker (north Bastar)": "Kanker",
    "Kanyakumari": "Kanniyakumari",
    "Karbi Anglong": "Karbi Anglong",
    "Kumarambheem Asifabad": "Kumuram Bheem Asifabad",
    "Kushinagar": "Kushi Nagar",
    "Lahaul & Spitti": "Lahul And Spiti",
    "Lakshadweep": "Lakshadweep District",
    "Leh": "Leh Ladakh",
    "Mahbubnagar": "Mahabubnagar",
    "Malda": "Maldah",
    "Medchal–malkajgiri": "Medchal Malkajgiri",
    "Nicobar": "Nicobars",
    "North 24 Parganas": "24 Paraganas North",
    "South 24 Parganas": "24 Paraganas South",
    "North East Delhi": "North East",
    "North West Delhi": "North West",
    "Panchmahal (godhra)": "Panch Mahals",
    "Paschim Medinipur": "Medinipur East",
    "Paschim Medinipur (west Medinipur)": "Medinipur West",
    "Puducherry": "Pondicherry",
    "Purba Medinipur (east Medinipur)": "Purba Bardhaman",
    "Raebareli": "Rae Bareli",
    "Rangareddy": "Ranga Reddy",
    "Rudraprayag": "Rudra Prayag",
    "Sabarkantha": "Sabar Kantha",
    "Sahibganj": "Sahebganj",
    "Siddharthnagar": "Siddharth Nagar",
    "Sant Kabir Nagar": "Sant Kabeer Nagar",
    "Seraikela Kharsawan": "Saraikela Kharsawan",
    "South Andaman": "South Andamans",
    "South Delhi": "South",
    "South East Delhi": "South East",
    "South Salmara-mankachar": "South Salmara Mancachar",
    "South West Delhi": "South West",
    "Nilgiris": "The Nilgiris",
    "Tiruvallur": "Thiruvallur",
    "Tumkur": "Tumakuru",
    "Thoothukudi": "Tuticorin",
    "Udham  Singh  Nagar": "Udam Singh Nagar",
    "Uttara Kannada": "Uttar Kannad",
    "Uttarkashi": "Uttar Kashi",
    "Viluppuram": "Villupuram",
    "Visakhapatnam": "Visakhapatanam",
    "Yamuna Nagar": "Yamunanagar",
    "Mehsana": "Mahesana",
    "Yadadri": "Yadadri Bhuvanagiri",
    "Nellore": "Spsr Nellore",
    "Sri Muktsar Sahab": "Sri Muktsar Sahib",
    "Balasore": "Baleshwar",
    "Aheri": "Kheri",
    "Sipahijala": "Sepahijala",
    "Belgaum": "Belagavi",
    "Rajouri": "Rajauri",
    "Sahibzada Ajit Singh Nagar": "Shahid Bhagat Singh Nagar",
    "East Delhi": "East",
    "West Delhi": "West",
    "Morigaon": "Marigaon",
    "West Sikkim": "West District",
    "South Sikkim": "South District",
    "East Sikkim": "East District",
    "North Sikkim": "North District",
    "North Delhi": "North",
    "Kadapa": "Y.s.r.",
    "South Dinajpur": "Dinajpur Dakshin",
    "Kawardha": "Kabirdham",
    "Sri Ganganagar": "Ganganagar",
    "S.a.s Nagar": "S.a.s Nagar",
    "North Dinajpur": "Dinajpur Uttar",
    "Kutch": "Kachchh",
    "Sant Ravidas Nagar": "Bhadohi",
    "Gulbarga": "Kalaburagi",
    "Kanshiram Nagar": "Kasganj",
    "East Champaran": "Purbi Champaran",
    "Khandwa": "East Nimar",
    "West Champaran": "Pashchim Champaran",
    "Bardhaman": "Purba Bardhaman"
}

# MongoDB source configuration for user collection
source_connection = mongo_connect('temp', 'district_slash_fix_source')

# MongoDB target configuration for user collection
target_collection = mongo_connect('temp', 'districts_ALL_FIXED')

# MongoDb source matching field
source_collection1 = mongo_connect('temp', 'district_slash_fix')

missing_data = []

missing_count = 0

count = 0

for x in source_connection.find({}):

    y = source_collection1.find_one({"name": x['name']})

    if y is None:

        if x['name'] in missing_dictionary.keys():
            temp = x['name']
            x['name'] = missing_dictionary[temp]

            y = source_collection1.find_one({"name": x['name']})

    if y is None:

        missing_data.append(x['name'])
        missing_count += 1

    else:
        z = target_collection.insert_one(
            {
                "_id": x['_id'] if '_id' in x else "",
                "code": y['code'],
                "name": y['name'] if 'name' in y else "",
                "short_name": y['short_name'] if 'short_name' in y else "",
                "state_id": x['state_id'],
                "slug": y['slug'] if 'slug' in y else "",
                "updated_at": y['updated_at'] if 'updated_at' in y else "",
                "created_at": y['created_at'] if 'created_at' in y else "",
                "import_batch": 1,
                "import_data": x
            }

        )

        count += 1

print("missed data" + str(missing_data))

print("added count: " + str(count))

print("missing count: " + str(missing_count))
