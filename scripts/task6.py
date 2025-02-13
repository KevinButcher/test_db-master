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

query = '''SELECT t.title, SUM(s.salary) AS Total_Expense, COUNT(t.title) As Employee_Count
            FROM titles t
            LEFT JOIN employees e ON t.emp_no = e.emp_no
            LEFT JOIN salaries s ON t.emp_no = s.emp_no
            WHERE t.to_date = (SELECT MAX(t1.to_date)
                                FROM titles t1
                                WHERE t1.emp_no = t.emp_no)
                                AND s.to_date = (SELECT MAX(s1.to_date)
                                                    FROM salaries s1 WHERE s1.emp_no = s.emp_no)
                                AND emp_status = 'E' AND s.to_date = '9999-01-01'
            GROUP BY t.title WITH ROLLUP
            ORDER BY Total_Expense;'''
cursor.execute(query)
result = cursor.fetchall()
first = "Job Title"
second = "Total Expense"
third = "Employee Count"
print("\t\t\t\tSalary Expense Report")
print("-----------------------------------------------------------")
print(f"|{first:20}|{second:20}|{third:14}|")
print("-----------------------------------------------------------")
for row in result:
    first = '%s' % row[0]
    second = '%s' % format(row[1], ",")
    third = '%s' % format(row[2], ",")
    none = "Total"
    if row[0] == None:
        print(f"|{none:20}|${second:20}|{third:14}|")
    else:
        print(f"|{first:20}|${second:20}|{third:14}|")
print("-----------------------------------------------------------")
print("Comments:")
print("This report assumes employees who have a final salary date are no longer employed.")
print("This report assumes a full year of expenses.")
print("This report assumes that the request to multiply the total yearly job title expenses by the number of employees"
      "was a miscommunication")
print("Summary:")
print("The job title with the most business expense will be Senior Staff and the least expense will be Managers")
print("The total yearly salary expense will be at the bottom of the report")
cnx.close()
