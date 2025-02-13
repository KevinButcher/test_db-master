from mysql.connector import connect, Error
from secrets import secrets
import csv

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

# create columns for emp_status and termination_date

query = '''ALTER TABLE employees
            ADD COLUMN emp_status CHAR(1) NOT NULL DEFAULT 'E' '''
cursor.execute(query)

query = '''ALTER TABLE employees
            ADD COLUMN termination_date DATE'''
cursor.execute(query)

# create procedure to terminate and update termination date

query = '''DROP PROCEDURE IF EXISTS update_termination;
            CREATE PROCEDURE update_termination(IN employee_no INT)
            BEGIN
                IF NOT EXISTS(SELECT emp_no FROM employees WHERE emp_no = employee_no)
                    THEN SIGNAL SQLSTATE '45000'
                        SET MESSAGE_TEXT = 'No employee was found.';
                END IF;
                UPDATE employees
                SET emp_status = 'T'
                WHERE emp_no = employee_no;
                UPDATE employees
                SET termination_date = CURDATE()
                WHERE emp_no = employee_no;
            END;'''
cursor.execute(query)

# Read csv file and terminate employees whose number matches

with open('employees_cuts.csv', newline='') as csvfile:
    cutreader = csv.reader(csvfile)
    next(cutreader, None)
    result = cutreader
    for row in result:
        query = '''CALL update_termination(%s)''' % int(row[0])
        try:
            cursor.execute(query)
            cnx.commit()
        except Error as e:
            cnx.rollback()
            print(e)

cnx.close()
