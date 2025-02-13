# from getpass import getpass
from pprint import pprint

from mysql.connector import connect, Error
from secrets import secrets

secret_key = secrets.get('SECRET_KEY')
db_user = secrets.get('DATABASE_USER', 'root')
db_pass = secrets.get('DATABASE_PASSWORD', 'pass')
db_port = secrets.get('DATABASE_PORT', 3306)

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

# cursor.execute("SELECT * FROM EMPLOYEES LIMIT 10;")
# result = cursor.fetchall()
# for row in result:
#     pprint(str(row[0]) + " %s" % row[2] + " %s" % row[3])

# cursor.execute("")

cnx.close()

