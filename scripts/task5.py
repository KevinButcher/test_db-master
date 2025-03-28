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

# Create procedure to determine and execute raises for remaining employees

query = '''DROP PROCEDURE IF EXISTS restructure_raises;
            CREATE PROCEDURE restructure_raises(IN employee_no INT, IN employee_title VARCHAR(50),
            IN current_salary INT, IN raise_day DATE)
            BEGIN
                IF raise_day != '9999-01-01'
                    THEN SIGNAL SQLSTATE '45000'
                        SET MESSAGE_TEXT = 'Employee no longer works here.';
                END IF;
                UPDATE salaries
                SET to_date = CURDATE()
                WHERE emp_no = employee_no AND to_date = '9999-01-01';
                CASE
                    WHEN employee_title = 'Assistant Engineer'
                    THEN INSERT INTO salaries (emp_no, salary, from_date, to_date)
                        VALUES (employee_no, current_salary * 1.05, CURDATE(), '9999-01-01');
                    WHEN employee_title = 'Engineer'
                    THEN INSERT INTO salaries (emp_no, salary, from_date, to_date)
                        VALUES (employee_no, current_salary * 1.075, CURDATE(), '9999-01-01');
                    WHEN employee_title = 'Manager'
                    THEN INSERT INTO salaries (emp_no, salary, from_date, to_date)
                        VALUES (employee_no, current_salary * 1.1, CURDATE(), '9999-01-01');
                    WHEN employee_title = 'Senior Engineer'
                    THEN INSERT INTO salaries (emp_no, salary, from_date, to_date)
                        VALUES (employee_no, current_salary * 1.07, CURDATE(), '9999-01-01');
                    WHEN employee_title = 'Senior Staff'
                    THEN INSERT INTO salaries (emp_no, salary, from_date, to_date)
                        VALUES (employee_no, current_salary * 1.065, CURDATE(), '9999-01-01');
                    WHEN employee_title = 'Staff'
                    THEN INSERT INTO salaries (emp_no, salary, from_date, to_date)
                        VALUES (employee_no, current_salary * 1.05, CURDATE(), '9999-01-01');
                    WHEN employee_title = 'Technique Leader'
                    THEN INSERT INTO salaries (emp_no, salary, from_date, to_date)
                        VALUES (employee_no, current_salary * 1.08, CURDATE(), '9999-01-01');
                END CASE;
            END;'''
cursor.execute(query)

# Select all remaining employees, their current titles, and their current salaries and then pass them
# to restructure_raises procedure

query = '''SELECT t.emp_no, t.title, s.salary, s.to_date
            FROM titles t
            LEFT JOIN employees e ON t.emp_no = e.emp_no
            LEFT JOIN salaries s ON t.emp_no = s.emp_no
            WHERE t.to_date = (SELECT MAX(t1.to_date)
                                FROM titles t1
                                WHERE t1.emp_no = t.emp_no)
                                AND s.to_date = (SELECT MAX(s1.to_date)
                                                    FROM salaries s1 WHERE s1.emp_no = s.emp_no)
                                AND emp_status = 'E';'''
cursor.execute(query)
result = cursor.fetchall()
for row in result:
    # title = row[1]
    query = '''CALL restructure_raises(%s, '%s', %s, '%s')''' % (row[0], row[1], row[2], row[3])
    try:
        cursor.execute(query)
        cnx.commit()
    except Error as e:
        cnx.rollback()
        print(e)

cnx.close()
