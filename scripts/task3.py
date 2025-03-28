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

# Create needed columns for comp_email, pers_email, and phone number

query = '''ALTER TABLE employees
            ADD COLUMN comp_email VARCHAR(255) NOT NULL DEFAULT 'procedure1';'''
cursor.execute(query)
query = '''ALTER TABLE employees
            ADD COLUMN pers_email VARCHAR(255);'''
cursor.execute(query)
query = '''ALTER TABLE employees
            ADD COLUMN comp_phone VARCHAR(12) NOT NULL DEFAULT 'procedure1';'''
cursor.execute(query)

# Create procedure to update personal and company emails utilizing function calls

query = '''DROP PROCEDURE IF EXISTS create_email;
            CREATE PROCEDURE create_email (IN employee_no INT)
            BEGIN
            CASE
                WHEN EXISTS(SELECT title
                        FROM titles
                        WHERE emp_no = employee_no AND title Like '%senior%')
                        THEN UPDATE employees
                        SET comp_email = company_email_creation(employee_no)
                        WHERE emp_no = employee_no;
                        UPDATE employees
                        SET pers_email = personal_email_creation(employee_no)
                        WHERE emp_no = employee_no;
                ELSE UPDATE employees
                    SET comp_email = company_email_creation(employee_no)
                    WHERE emp_no = employee_no;
            END CASE;
            END;
    '''
cursor.execute(query)

# create procedure to create personal email upon senior promotion

query = '''DROP PROCEDURE IF EXISTS create_personal_email;
            CREATE PROCEDURE create_personal_email(IN employee_no INT)
            BEGIN
                UPDATE employees
                SET pers_email = personal_email_creation(employee_no)
                WHERE emp_no = employee_no;
            END;'''
cursor.execute(query)

# Create phone procedure

query = '''DROP PROCEDURE IF EXISTS create_phone;
            CREATE PROCEDURE create_phone(IN employee_no INT, IN ID_count INT)
            BEGIN
                CASE
                    WHEN ID_count = 5
                    THEN UPDATE employees
                        SET comp_phone = CONCAT('801-60', LEFT(employee_no,1), '-', RIGHT(employee_no,4))
                        WHERE emp_no = employee_no;
                    ELSE UPDATE employees
                        SET comp_phone = CONCAT('801-6', LEFT(employee_no,2), '-', RIGHT(employee_no,4))
                        WHERE emp_no = employee_no;
                END CASE;
            END;'''
cursor.execute(query)

# create procedure used to insert new rows into table that will automatically populate emails and phone

query = '''DROP PROCEDURE IF EXISTS new_employee;
            CREATE PROCEDURE new_employee(IN employee_no INT, IN birthday DATE, IN firstName VARCHAR(14),
                IN lastName VARCHAR(16), IN _gender CHAR(1), IN hireDate DATE)
            BEGIN
                DECLARE phone INT;
                SET phone = CHAR_LENGTH(employee_no);
                INSERT INTO employees (emp_no, birth_date, first_name, last_name, gender, hire_date)
                VALUES (employee_no, birthday, firstName, lastName, _gender, hireDate);
                CASE
                    WHEN phone = 5
                    THEN UPDATE employees
                        SET comp_phone = CONCAT('801-60', LEFT(employee_no, 1), '-', RIGHT(employee_no,4))
                        WHERE emp_no = employee_no;
                    ELSE UPDATE employees
                        SET comp_phone = CONCAT('801-6', LEFT(employee_no,2), '-', RIGHT(employee_no,4))
                        WHERE emp_no = employee_no;
                END CASE;
                CASE
                    WHEN EXISTS(SELECT title
                                FROM titles
                                WHERE emp_no = employee_no AND title LIKE '%senior%')
                                THEN UPDATE employees
                                SET comp_email = company_email_creation(employee_no)
                                WHERE emp_no = employee_no;
                                UPDATE employees
                                SET pers_email = personal_email_creation(employee_no)
                                WHERE emp_no = employee_no;
                    ELSE UPDATE employees
                    SET comp_email = company_email_creation(employee_no)
                    WHERE emp_no = employee_no;
                END CASE;
            END;'''
cursor.execute(query)

# Create function to handle company email creation

query = '''DROP FUNCTION IF EXISTS `company_email_creation`;
    CREATE FUNCTION `company_email_creation` (emp_id INT)
    RETURNS VARCHAR(255)
    DETERMINISTIC
    BEGIN
        DECLARE i INT;
        DECLARE beginning VARCHAR(255);
        SELECT LOWER(CONCAT(LEFT(first_name,1),last_name)) INTO beginning
        FROM employees
        WHERE emp_no = emp_id;
        SET i = 0;
        CASE
            WHEN EXISTS (SELECT comp_email
                FROM employees
                WHERE comp_email = CONCAT(beginning, '@company.net'))
                THEN SET i = i+1;
                WHILE (EXISTS(SELECT comp_email FROM employees WHERE comp_email = LOWER(CONCAT(beginning, i, '@company.net'))))
                DO SET i = i+1;
                END WHILE;
                RETURN CONCAT(beginning, i, '@company.net');
            ELSE RETURN CONCAT(beginning, '@company.net');
        END CASE;
    END;'''
cursor.execute(query)

# Create function to handle personal email creation

query = '''DROP FUNCTION IF EXISTS `personal_email_creation`;
            CREATE FUNCTION `personal_email_creation` (emp_id INT)
            RETURNS VARCHAR(255)
            DETERMINISTIC
            BEGIN
                DECLARE i INT;
                DECLARE beginning VARCHAR(255);
                SELECT LOWER(CONCAT(LEFT(first_name,1), last_name)) INTO beginning
                FROM employees
                WHERE emp_no = emp_id;
                SET i = 0;
                CASE
                    WHEN EXISTS (SELECT pers_email
                        FROM employees
                        WHERE pers_email = CONCAT(beginning, '@personal.com'))
                        THEN SET i = i+1;
                        WHILE (EXISTS(SELECT pers_email FROM employees WHERE pers_email = LOWER(CONCAT(beginning, i, '@personal.com'))))
                        DO SET i = i+1;
                        END WHILE;
                        RETURN CONCAT(beginning, i, '@personal.com');
                    ELSE RETURN CONCAT(beginning, '@personal.com');
                END CASE;
            END;'''
cursor.execute(query)

# create trigger to populate personal email upon promotion

query = '''DROP TRIGGER IF EXISTS senior_promotion;
            CREATE TRIGGER senior_promotion
            AFTER UPDATE
            ON titles FOR EACH ROW
            BEGIN
                IF NEW.title LIKE '%senior%'
                    THEN CALL create_personal_email(NEW.emp_no);
                END IF;
            END;'''
cursor.execute(query)

# create trigger to populate personal email for newly hired senior management

query = '''DROP TRIGGER IF EXISTS senior_hire;
            CREATE TRIGGER senior_hire
            AFTER INSERT
            ON titles FOR EACH ROW
            BEGIN
                IF NEW.title LIKE '%senior%'
                    THEN CALL create_personal_email(NEW.emp_no);
                END IF;
            END;'''
cursor.execute(query)

# Execute creation of emails

query = '''SELECT
                emp_no
            FROM
                employees
            order by last_name, emp_no;'''
cursor.execute(query)
result = cursor.fetchall()
for row in result:
    query = '''CALL create_email(%s)''' % (row[0])
    cursor.execute(query)
    cnx.commit()

# execute creation of company phone numbers

query = ''' SELECT emp_no, CHAR_LENGTH(emp_no)
            FROM employees
            ORDER BY last_name, emp_no;'''
cursor.execute(query)
result = cursor.fetchall()
for row in result:
    query = '''CALL create_phone(%s, %s)''' % (row[0], row[1])
    cursor.execute(query)
    cnx.commit()

cnx.close()

