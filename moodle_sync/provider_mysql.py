# file: moodle_sync/provider_mysql.py

from functools import partial, lru_cache


import cryptography # this is a non-included dependency package of pymysql
import pymysql

# import pydantic


from typing import List, Dict, Annotated, Iterable, Union

from moodle_sync.course import MoodleCourseProvider
from moodle_sync.enrolment import MoodleEnrolmentProvider
from moodle_sync.course import MoodleCourseProvider
from moodle_sync.config import config
from moodle_sync.logger import logger

class Mysql:

    _instances = {}

    r"""
    Usage

        with Mysql(host, user, password, database) as mysql:
            result = mysql.query('SELECT * FROM your_table')
            print(result)

    or...
        mysql = Mysql(host, user, password, database)
        with mysql:
            result = mysql.select('SELECT * FROM your_table')
            print(result)

    or to manually deal with the connection ...
        mysql = Mysql(host, user, password, database)
        mysql.connect()
        result = mysql.select('SELECT * FROM your_table')
        mysql.close()
        mysql.connect(database='your_other_db')  # you can change just the DB. Or any of the parameters.


    """

    def __new__(cls, host:str, database:str, user:str, password:str):
        """
        Implement a singleton pattern for the Mysql class that offers a single instance for each connection.
        Allows one connection for each host / user / db combination.
        :param site:
        :param api_key:
        """
        instance_id = f"{host}:{user}:{database}"
        if instance_id not in cls._instances:
            cls._instances[instance_id] = super(Mysql, cls).__new__(cls)
        else:
            pass

        return cls._instances[instance_id]
    def __init__(self,  host:str, database:str, user:str, password:str):
        if hasattr(self, '__initialized'):
            # already initialized.
            return
        self.connection_parameters = {'host': host, 'user': user, 'password': password, 'database': database}
        self.__connection = None
        self.columns = None
        self.__initialized = True
        self.last_query = None
        self.last_params = None

    def connect(self, **connection_parameters):
        """
        Optinally updates any of the connectoin parameters on a new connection.
        Connection must be closed otherwise the existing connection will be returned.
        @param connection_parameters:  {'host': host, 'user': user, 'password': password, 'database': database}
        @return: the sql connection
        """
        self.connection_parameters.update(connection_parameters)
        if self.__connection:
            try:
                if self.__connection.ping():
                    return self
            except:
                pass
        self.__connection = pymysql.connect(**self.connection_parameters)
        return self.__connection

    def close(self):
        if self.__connection:
            self.__connection.close()
            self.__connection = None

    def close(self):
        if self.__connection:
            self.__connection.close()
            self.__connection = None

    def __enter__(self):
        self.connect()
        self.__connection.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            # If no exception occurred, commit the transaction
            self.__connection.commit()
        else:
            # If an exception occurred, rollback the transaction
            self.__connection.rollback()
        self.close()


    def select(self, select: str, params: Union[Dict,tuple,None] = None) -> List[dict]:
        """
        return rows as a list.  Each row is a tuple that can be converted to a dict with row_to_dict()
        Gathers and stores the column information for later use
                in self.columns (just column names)
                and self.column_details
                (<ColumnName>, <Type>,   <DisplaySize>, <InternalSize>, <Precision>, <Scale>, <Nullable> )
                ('AppID  ', <class 'int'>,     None,      10,              10,         0,          False)

        result = instance.query("SELECT * FROM users WHERE username = %(username)s and password = %(password)s",
                        {'username': 'test_user', 'password': 'test_password'})

        result = instance.select("SELECT * from my_table WHERE column1 LIKE %s", ('%' + my_value + '%',))

        @param query: a string for the query
        @param params: query parameters to substitute in to the query. If it is a dict, use %(key)s placeholders.
            If it is a tuple, use %s placeholders.

        @return: a list of dicts of rows (rows with row headers as keys)
        """
        self.last_query = select
        self.last_params = params
        temp_connection = False
        if self.__connection is None:
            temp_connection = self.connect()

        try:
            with self.__connection.cursor() as cursor:
                cursor.execute(select, params)
                # a cursor.description row like this:  ('APPID', <class 'int'>, None, 10, 10, 0, False)
                self.columns = [d[0] for d in cursor.description]
                result = cursor.fetchall()

        finally:
            if temp_connection:
                self.close()

        dict_result = [self.row_to_dict(row) for row in result]
        return dict_result

    def query(self, query: str, params: Union[Dict,tuple,List[tuple],None] = None, dryrun_result = None) -> int:
        """
        Execute a SQL query with optional parameters
        Examples:
        result = instance.query("SELECT * FROM users WHERE username = %(username)s and password = %(password)s",
                        {'username': 'test_user', 'password': 'test_password'})


        :param query: a string for the SQL query with placeholders for parameters
        :param params: optional dict of parameters to substitute in to the query
                    If the parameter is a list of tuples, executemany will be used.
        :return: int (number of rows affected).  On error, will cause an exception.
            If a temporary connection, exception will cause a rollback.
        """
        self.last_query = query
        self.last_params = params
        if config.debug:
            logger.debug("Query", query)
            logger.debug("Params", params)
        if config.dryrun:
            return dryrun_result
        temp_connection = False
        if self.__connection is None:
            temp_connection = self.connect()

        try:
            with self.__connection.cursor() as cursor:
                # is this a good way to see if we executemany?
                if isinstance(params, list) and all(isinstance(i, tuple) for i in params):
                    cursor.executemany(query, params)
                else:
                    cursor.execute(query, params)
                if temp_connection:
                    self.__connection.commit()

        except Exception as e:
            self.__connection.rollback()
            raise e
        finally:
            if temp_connection:
                self.close()

        result = cursor.rowcount
        return result


    def row_headers(self, row: tuple) -> [str]:
        return self.columns

    def row_to_dict(self, row: ()) -> dict:
        headers = self.columns
        return {x[0]: x[1] for x in zip(headers, row)}


class MoodleMySQLCourseProvider(MoodleCourseProvider):
    """
    Based on Moodle 4.1 Schema
    """
    def __init__(self, host, user, password, database):
        super().__init__()
        self.mysql = Mysql(host=host, database=database, user=user, password=password)
        self.convert_dates = True

    def get_courses(self,  field: Union[str,None] = None, value: Union[str,None] = None) -> List[Dict]:
        """
        Get all courses from the database.  Optionally filter by field and value.
        :param field: a moodle course field
        :param value: value to filter for.  Accepts wildcards
        :return:
        """
        query = """
        SELECT
            c.id, c.shortname, c.fullname, c.idnumber, c.category as categoryid,
               c.summary, c.startdate, c.enddate, c.format, c.showgrades,
               c.newsitems,  c.visible,
               cfo_n.value as numsections,
               cfo_a.value as automaticenddate
        FROM mdl_course c
        LEFT JOIN    mdl_course_format_options cfo_a ON c.id = cfo_a.courseid AND cfo_a.name = 'automaticenddate'
        LEFT JOIN    mdl_course_format_options cfo_n ON c.id = cfo_n.courseid AND cfo_n.name = 'numsections'
        """

        params = []
        if field and value:
            valid_fields = {'id', 'shortname', 'fullname', 'idnumber', 'category', 'format', 'visible'}
            if field not in valid_fields:
                raise ValueError(f"Invalid field name: {field}")

            value = str(value)
            if  '%' in value or '_' in value:
                query += f" WHERE LOWER(c.{field}) LIKE LOWER(%s)"
            else:
                query += f" WHERE c.{field} = %s"
            params.append(value)

        with self.mysql as conn:
            courses = conn.select(query, tuple(params))

        if self.convert_dates:
            for course in courses:
                course['startdate'] = int(course['startdate'])
                course['enddate'] = int(course['enddate'])

        self.courses = courses
        return self.courses

    def get_course(self, shortname_or_id: Union[str, int]) -> Union[dict, None]:
        if isinstance(shortname_or_id, int) or shortname_or_id.isdigit():
            field = 'id'
            value = int(shortname_or_id)
        else:
            field = 'shortname'
            value = shortname_or_id

        courses = self.get_courses(field, value)

        if not courses:
            return None

        course = courses[0]
        if self.convert_dates:
            course['startdate'] = int(course['startdate'])
            course['enddate'] = int(course['enddate'])

        return course

    def create_course(self, course: Dict) -> Union[int, None]:
        """
        Note that the mysql provider does not duplicate a course from a template.
        It just creates a blank course.  This may or may not work.
        :param course:
        :return:
        """
        existing_course = self.get_course(course['shortname'])
        if existing_course:
            raise ValueError(f"Course already exists with shortname: {course['shortname']}")

        query = """
        INSERT INTO mdl_course (
            shortname, fullname, idnumber, category, summary, summaryformat, 
            format, showgrades, newsitems, startdate, enddate, 
            visible, timecreated, timemodified
        ) VALUES (
            %(shortname)s, %(fullname)s, %(idnumber)s, %(categoryid)s, %(summary)s, 1,
            %(format)s, %(showgrades)s, %(newsitems)s, %(startdate)s, %(enddate)s,
            %(visible)s, UNIX_TIMESTAMP(), UNIX_TIMESTAMP()
        )
        """
        with self.mysql as conn:
            course_id = conn.query(query, course)

        if course_id:
            self.update_course(course, force_all_fields=True, course_id=course_id)
        return course_id

    def update_course(self, course: Dict, force_all_fields=False, course_id: Union[int, None] = None):
        if course_id is None:
            existing_course = self.get_course(course['shortname'])
            if not existing_course:
                raise ValueError(f"Course not found: {course['shortname']}")
            course_id = existing_course['id']

        fields_to_update = self.fields_to_update if not force_all_fields else self.fields

        update_fields = []
        update_values = []
        for field in fields_to_update:
            if field in course:
                update_fields.append(f"{field} = %s")
                update_values.append(course[field])

        if 'automaticenddate' in course:
            self._update_course_format_option(course_id, 'automaticenddate', course['automaticenddate'], course.get('format', 'weeks'))
        if 'numsections' in course:
            self._update_course_format_option(course_id, 'numsections', course['numsections'], course.get('format', 'topics'))

        query = f"""
        UPDATE mdl_course
        SET {', '.join(update_fields)}, timemodified = UNIX_TIMESTAMP()
        WHERE id = %s
        """
        update_values.append(course_id)

        with self.mysql as conn:
            conn.query(query, tuple(update_values))

    def _update_course_format_option(self, course_id: int, name: str, value: Union[int, str], format: str = 'weeks'):
        query = """
        INSERT INTO mdl_course_format_options (courseid, format, sectionid, name, value)
        VALUES (%s, %s, 0, %s, %s)
        ON DUPLICATE KEY UPDATE value = VALUES(value), format = VALUES(format)
        """
        with self.mysql as conn:
            conn.query(query, (course_id, format, name, value))

    def get_category(self, name_or_id: Union[str, int]) -> Union[int, None]:
        if isinstance(name_or_id, int) or (isinstance(name_or_id, str) and name_or_id.isdigit()):
            field = 'id'
            value = int(name_or_id)
        else:
            field = 'name'
            value = name_or_id

        query = f"SELECT id FROM mdl_course_categories WHERE {field} = %s"
        with self.mysql as conn:
            result = conn.select(query, (value,))

        return result[0]['id'] if result else None

    def create_category(self, category_name: str, category_parent_name: Union[str, None] = None) -> int:
        parent_id = self.get_category(category_parent_name) if category_parent_name else 0

        query = """
        INSERT INTO mdl_course_categories (name, parent, sortorder, visible, timemodified)
        VALUES (%s, %s, 
            (SELECT COALESCE(MAX(sortorder), 0) + 1 FROM mdl_course_categories WHERE parent = %s), 
            1, UNIX_TIMESTAMP())
        """
        with self.mysql as conn:
            category_id = conn.query(query, (category_name, parent_id, parent_id))

        return category_id


from typing import List, Dict, Union
from moodle_sync.enrolment import MoodleEnrolmentProvider
from moodle_sync.config import config
from moodle_sync.provider_mysql import Mysql


class MoodleMySQLEnrolmentProvider(MoodleEnrolmentProvider):
    def __init__(self, host, user, password, database):
        super().__init__()
        self.mysql = Mysql(host=host, database=database, user=user, password=password)
        self.roles_to_sync = ['student', 'editingteacher']

    @lru_cache(maxsize=None)
    def get_user_id(self, username: str) -> Union[None, int]:
        query = "SELECT id FROM mdl_user WHERE username = %s"
        with self.mysql as conn:
            result = conn.select(query, (username,))
        return result[0]['id'] if result else None

    @lru_cache(maxsize=None)
    def get_username(self, user_id: int) -> Union[None, str]:
        query = "SELECT username FROM mdl_user WHERE id = %s"
        with self.mysql as conn:
            result = conn.select(query, (user_id,))
        return result[0]['username'] if result else None


    @lru_cache(maxsize=None)
    def get_role_id(self, role: str) -> Union[None, int]:
        query = "SELECT id FROM mdl_role WHERE shortname = %s"
        with self.mysql as conn:
            result = conn.select(query, (role,))
        return result[0]['id'] if result else None

    @lru_cache(maxsize=None)
    def get_course_id(self, shortname: str) -> Union[None, int]:
        query = "SELECT id FROM mdl_course WHERE shortname = %s"
        with self.mysql as conn:
            result = conn.select(query, (shortname,))
        return result[0]['id'] if result else None

    def get_enroled_users(self, course: Union[str, int]) -> List[Dict[str, int]]:
        """
        This version's return value also includes role as text.
        :param course: shortname or course_id
        :return: with user_id, course_id, and role_id (minimally)
        """
        course_id = self.get_course_id(course) if isinstance(course, str) else course
        if course_id is None:
            raise ValueError(f"Course does not exist: {course}")

        # Get all the users even if they are "suspended" (have no role)
        query = """
        SELECT DISTINCT
            u.id as user_id,
            u.username,
            c.id as course_id,
            c.shortname as course_shortname,
            COALESCE(r.id, 0) as role_id,
            r.shortname,
            ue.status as enrolment_status,
            e.enrol as enrolment_method
        FROM mdl_user u
        JOIN mdl_user_enrolments ue ON u.id = ue.userid
        JOIN mdl_enrol e ON ue.enrolid = e.id
        JOIN mdl_course c ON e.courseid = c.id
        LEFT JOIN mdl_context ctx ON ctx.instanceid = c.id AND ctx.contextlevel = 50
        LEFT JOIN mdl_role_assignments ra ON u.id = ra.userid AND ctx.id = ra.contextid
        LEFT JOIN mdl_role r ON ra.roleid = r.id
        -- WHERE c.id = 5612
        WHERE c.id = %s AND (r.shortname IN %s or r.shortname is NULL)
        """
        with self.mysql as conn:
            result = conn.select(query, (course_id, self.roles_to_sync))
        return result

    def course_enrol_user(self, user: Union[int, str], course: Union[str, int], role: Union[str, int] = 'student') -> \
    Dict[str, int]:
        user_id = self.get_user_id(user) if isinstance(user, str) else user
        if user_id is None:
            raise ValueError(f"User does not exist: {user}")

        course_id = self.get_course_id(course) if isinstance(course, str) else course
        if course_id is None:
            raise ValueError(f"Course does not exist: {course}")

        role_id = self.get_role_id(role) if isinstance(role, str) else role
        if role_id is None:
            raise ValueError(f"Role does not exist: {role}")

        # First, ensure the user is enrolled in the course
        enrol_query = """
        INSERT IGNORE INTO mdl_user_enrolments (status, enrolid, userid, timestart, timeend, modifierid, timecreated, timemodified)
        SELECT 0, e.id, %s, UNIX_TIMESTAMP(), 0, 2, UNIX_TIMESTAMP(), UNIX_TIMESTAMP()
        FROM mdl_enrol e
        WHERE e.courseid = %s AND e.enrol = 'manual'
        LIMIT 1
        """

        # Then, assign the role
        role_query = """
        INSERT IGNORE INTO mdl_role_assignments (roleid, contextid, userid, timemodified)
        SELECT %s, ctx.id, %s, UNIX_TIMESTAMP()
        FROM mdl_context ctx
        WHERE ctx.contextlevel = 50 AND ctx.instanceid = %s
        """

        with self.mysql as conn:
            enrol_cnt = conn.query(enrol_query, (user_id, course_id))
            roll_cnt = conn.query(role_query, (role_id, user_id, course_id))

        return {"user_id": user_id, "course_id": course_id, "role_id": role_id,
                "num_new_enrols": enrol_cnt, "num_roles_added": roll_cnt}

    def course_unenrol_user(self, user: Union[int, str], course: Union[int, str], role: Union[int, str]) -> dict:
        user_id = self.get_user_id(user) if isinstance(user, str) else user
        if user_id is None:
            raise ValueError(f"User does not exist: {user}")

        course_id = self.get_course_id(course) if isinstance(course, str) else course
        if course_id is None:
            raise ValueError(f"Course does not exist: {course}")

        role_id = self.get_role_id(role) if isinstance(role, str) else role
        if role_id is None:
            raise ValueError(f"Role does not exist: {role}")

        query = """
        DELETE ra FROM mdl_role_assignments ra
        JOIN mdl_context ctx ON ra.contextid = ctx.id
        WHERE ra.userid = %s AND ctx.instanceid = %s AND ctx.contextlevel = 50 AND ra.roleid = %s
        """

        with self.mysql as conn:
            changed_rows = conn.query(query, (user_id, course_id, role_id))

        return {"user_id": user_id, "course_id": course_id, "role_id": role_id, "num_roles_deleted": changed_rows}

    def course_delete_user(self, user: Union[int, str], course: Union[str, int]) -> Dict[str, int]:
        user_id = self.get_user_id(user) if isinstance(user, str) else user
        if user_id is None:
            raise ValueError(f"User does not exist: {user}")

        course_id = self.get_course_id(course) if isinstance(course, str) else course
        if course_id is None:
            raise ValueError(f"Course does not exist: {course}")

        # delete all roles and then delete enrolment itself.
        query1 = """
        DELETE ra FROM mdl_role_assignments ra
            JOIN mdl_context ctx ON ra.contextid = ctx.id
            WHERE ra.userid = %s AND ctx.instanceid = %s AND ctx.contextlevel = 50
        """

        query2 = """
        DELETE ue FROM mdl_user_enrolments ue
            JOIN mdl_enrol e ON ue.enrolid = e.id
            WHERE ue.userid = %s AND e.courseid = %s
        """

        with self.mysql as conn:  # note - auto rollback
            roles_deleted = conn.query(query1, (user_id, course_id))
            participates_deleted = conn.query(query2, (user_id, course_id))

        return {"user_id": user_id, "course_id": course_id,
                "num_roles_deleted": roles_deleted, "num_participations_deleted": participates_deleted }



