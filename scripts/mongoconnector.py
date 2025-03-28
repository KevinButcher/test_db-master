from pprint import pprint

import certifi as certifi
from pymongo import MongoClient
import urllib.parse
from striprtf.striprtf import rtf_to_text

first = "mongodb+srv://myAtlasDBUser:"
password = 'Password'
last = "@myatlasclusteredu.g0rxxs2.mongodb.net/?retryWrites=true&w=majority"
encode = urllib.parse.quote_plus(password)
MONGODB_URI = first + encode + last

print(MONGODB_URI)

client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())

with open('employee appreciation database .rtf', newline='') as rtffile:
    bonusreader = rtffile.read()
    text = rtf_to_text(bonusreader)
    print(text)
    # temp = text.split('\n')
    # print(temp)
#     result = []
#     for row in temp:
#         if row != '':
#             result.append(row)
# fixed = ' '.join(result)
# print(fixed)

mydb = client["employee_appreciation"]
mycol = mydb["bonuses"]
mydict =[
    { "yearsOfService": 1, "bonusAmount": 50 },
    { "yearsOfService": 5, "bonusAmount": 500 },
    { "yearsOfService": 10, "bonusAmount": 1000 },
    { "yearsOfService": 15, "bonusAmount": 1500 },
    { "yearsOfService": 20, "bonusAmount": 3000 },
    { "yearsOfService": 25, "bonusAmount": 4000 },
    { "yearsOfService": 30, "bonusAmount": 5000 }
]
insert = mycol.insert_many(mydict)

for db_name in client.list_database_names():
    print(db_name)

client.close()
