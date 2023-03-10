import sqlite3
from sqlite3 import Error
db = sqlite3.connect('')


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn


def execute_sql(conn, sql):
    """ Execute sql
    :param conn: Connection object
    :param sql: a SQL script
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def add_student(conn, students_list):
    """
    add a new student into the students table
    :param conn:
    :param student:
    :return: student id
    """
    sql = '''INSERT or IGNORE INTO students(id, surname, name,  age, class)
            VALUES(?,?,?,?,?)'''
    cur = conn.cursor()
    cur.executemany(sql, students_list)
    conn.commit()
    return cur.lastrowid


def add_subject(conn, subjects_list):
    """
    add a new subject into the subject table
    :param conn:
    :param subject:
    :return: subject id
    """
    sql = '''INSERT or IGNORE INTO subjects(id, student_id, subject, grade)
            VALUES(?,?,?,?)'''
    cur = conn.cursor()
    cur.executemany(sql, subjects_list)
    conn.commit()
    return cur.lastrowid


def select_all(conn, table):

    cur = conn.cursor()
    cur.execute(f"SELECT *FROM {table}")
    rows = cur.fetchall()
    return rows


def select_where(conn, table, **query):
    """
    Query tasks from table with data from **query dict
    :param conn: the Connection object
    :param table: table name
    :param query: dict of attributes and values
    :return:
    """
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    return rows


def update(conn, table, id, **kwargs):
    """
    update surname, name, age, class of a student
    :param conn:
    :param table: table name
    :param id: row id
    :return:
    """
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id, )

    sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("OK")
    except sqlite3.OperationalError as e:
        print(e)


def delete_where(conn, table, **kwargs):
    """
    Delete from table where attributes from
    :param conn:  Connection to the SQLite database
    :param table: table name
    :param kwargs: dict of attributes and values
    :return:
    """
    qs = []
    values = tuple()
    for k, v in kwargs.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)

    sql = f'DELETE FROM {table} WHERE {q}'
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    print("Deleted")


def delete_all(conn, table):
    """
    Delete all rows from table
    :param conn: Connection to the SQLite database
    :param table: table name
    :return:
    """
    sql = f'DELETE FROM {table}'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print("Deleted")


if __name__ == '__main__':

    create_students_sql = """
   -- students table
   CREATE TABLE IF NOT EXISTS students (
      id integer PRIMARY KEY,
      surname text NOT NULL,
      name text NOT NULL,
      age integer,
      class text
    )
    """

    create_subjects_sql = """
   -- subjects table
   CREATE TABLE IF NOT EXISTS subjects (
      id integer PRIMARY KEY,
      student_id integer NOT NULL,
      subject VARCHAR(250) NOT NULL,
      grade integer,
            
      FOREIGN KEY (student_id) REFERENCES students (id)
    )
    """

    db_file = "database.db"

    conn = create_connection(r"database.db")
    if conn is not None:
        execute_sql(conn, create_students_sql)
        execute_sql(conn, create_subjects_sql)

    students_list = [
        ("1", "Артеменко", "Олександр", "10", "5-D"),
        ("2", "Микитенко", "Дмитро", "10", "5-A"),
        ("3", "Сергієнко", "Віктор", "10", "5-С")
    ]

    student_id = add_student(conn, students_list)

    # for student in students_list:
    # student_id = add_student(conn, students_list)
    # print(student_id)

    # student = ("1", "Артеменко", "Олександр", "10", "5-D")
    # student = ("2", "Микитенко", "Дмитро", "10", "5-A")
    # student = ("3", "Сергієнко", "Віктор", "10", "5-С")

    subjects_list = [
        ("1", "1", "Математика", "6"),
        ("2", "2", "Математика", "5"),
        ("3", "3", "Математика", "4")
    ]

    subject_id = add_subject(conn, subjects_list)

    # for subject in subjects_list:
    # subject_id = add_subject(conn, subjects_list)
    # print(subject_id)

    # print(student_id, subject_id)
    conn.commit

    select_all(conn, "students")
    select_all(conn, "subjects")
    select_where(conn, "students", surname="Микитенко")
    select_where(conn, "subjects", subject="Математика")

    # update(conn, "students", 3, age=11)

    # delete_where(conn, "students", id=2)
    # delete_all(conn, "subjects")

    conn.close()

    # Додавання у змінну всього списку студентів у VALUES

    # add_student_sql = """
    # --insert into table students
    # INSERT or IGNORE INTO students(id, surname, name,  age, class)
    # VALUES(
    # "1", "Артеменко", "Олександр", "10", "5-D",
    # "2", "Микитенко", "Дмитро", "10", "5-A",
    # "3", "Сергієнко", "Віктор", "10", "5-С"
    # """

    # execute_sql(conn, add_student_sql)
