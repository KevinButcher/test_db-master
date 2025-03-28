from pprint import pprint

from mysql.connector import connect, Error
from secrets import secrets
import certifi as certifi
from pymongo import MongoClient
import urllib.parse
from striprtf.striprtf import rtf_to_text

secret_key = secrets.get('SECRET_KEY')
db_user = secrets.get('DATABASE_USER', 'root')
db_pass = secrets.get('DATABASE_PASSWORD', 'pass')
db_port = secrets.get('DATABASE_PORT', 3306)
mdb_pass = secrets.get('SECRET_KEY2')

try:
    cnx = connect(
        host='localhost',
        user=db_user,
        password=db_pass,
        database='employees'
    )
except Error as e:
    print(e)
cursor = cnx.cursor()
cursor.execute("SELECT DATABASE()")
data = cursor.fetchone()
print("Connection established to: ", data)

first = "mongodb+srv://myAtlasDBUser:"
last = "@myatlasclusteredu.g0rxxs2.mongodb.net/?retryWrites=true&w=majority"
encode = urllib.parse.quote_plus(mdb_pass)
MONGODB_URI = first + encode + last

client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())

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

db = client.employee_appreciation
bonus = db.bonuses
query = '''SELECT TIMESTAMPDIFF(YEAR, e.hire_date, CURDATE()), e.emp_no
            FROM employees e
            LEFT JOIN salaries s ON e.emp_no = s.emp_no
            WHERE s.to_date = '9999-01-01'
                AND e.emp_status = 'E'
                AND TIMESTAMPDIFF(YEAR, e.hire_date, CURDATE()) > 10;'''
cursor.execute(query)
result = cursor.fetchall()
for row in result:
    if 10 <= row[0] < 15:
        for documents in bonus.find({"yearsOfService": 10}, {"_id": 0}):
            print('Employee numbered %s' % row[1] + ' has worked for the company %s' % row[0] + ' years and earned: ')
            print(documents)
    elif 15 <= row[0] < 20:
        for documents in bonus.find({"yearsOfService": 15}, {"_id": 0}):
            print('Employee numbered %s' % row[1] + ' has worked for the company %s' % row[0] + ' years and earned: ')
            print(documents)
    elif row[0] >= 20:
        for documents in bonus.find({"yearsOfService": 20}, {"_id": 0}):
            print('Employee numbered %s' % row[1] + ' has worked for the company %s' % row[0] + ' years and earned: ')
            print(documents)

client.close()
cnx.close()
