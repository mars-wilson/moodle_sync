import pyodbc
import datetime

from typing import Union, Dict, List, Set

from moodle_sync.course import MoodleCourseProvider
from moodle_sync.enrolment import MoodleEnrolmentProvider
from moodle_sync.user import MoodleUserProvider

from moodle_sync.config import config

class MoodleMSSQLCourseProvider(MoodleCourseProvider):

    """
    The course table may contain all the courses that you might potentially want to sync to moodle
    But you can include more columns in the course table than would go into moodle, so that you can filter them
    when you actually do the syncing based on those values if you want.

    Extra fields are okay.

    Field types and values should be Moodle ready.
    """
    def __init__(self, connection_string:str, course_table:str=None):

        super().__init__()
        self.connection_string = connection_string
        self.course_table = course_table
        self.convert_dates = True  # do this by default, but it is an option.
        pass

    def get_courses(self):
        """
        Return a list of dictionaries of courses from a MSSQL course as specified in course_table.

        The keys in each dictionary should either match the source keys or the Moodle keys from CourseSync
        You can call this function at the END of your derived class if you want some sanity checks
        after setting the self.courses in your own get_courses implementation.

        :return: List[dict]:  list of courses with fields and values.
        """
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {self.course_table}")
            columns = [column[0] for column in cursor.description]
            types   = [column[1] for column in cursor.description]   # might be kinda interesting!
            data = cursor.fetchall()
        # wheeeeee! zip em up.
        courses = [dict(zip(columns, row)) for row in data]
        if self.convert_dates:
            courses = [self.convert_dates_timezone_unaware(course) for course in courses]
        self.courses = courses
        return self.courses


    def convert_dates_timezone_unaware(self, course):
        """

        Convert the dates in the course to unix timestamps.
        This is very difficult to do in SQL if the dates are in a local timezone to simply convert them to
        an equivalent time for Moodle because it's hard to figure out what daylight time is for a future date
        when doing that conversion.

        :param course: dict: course dictionary
        :return: dict: course dictionary
        """
        for key in course:
            if 'date' in key.lower():
                if isinstance(course[key], datetime.datetime):
                    original = course[key]
                    newvalue = int(original.timestamp())
                    newvalue_fromtimestamp = datetime.datetime.fromtimestamp(newvalue).strftime('%Y-%m-%d %H:%M:%S')
                    # print(f"Converting {original} to {newvalue} which seems to be {newvalue_fromtimestamp}")
                    course[key] = int(course[key].timestamp())
        return course


class MoodleMSSQLEnrolmentProvider(MoodleEnrolmentProvider):

    def __init__(self, connection_string: str, enrollment_table: str):
        """

        :param connection_string: The PYODBC connection string for the database
        :param enrollment_table: The table from which to pull enrollments
        :param user_table: the table from which to pull users.  None if no sync users.
        """
        super().__init__()
        self.connection_string = connection_string
        self.enrollment_table = enrollment_table



    def get_enroled_users(self, course: Union[str, int] = None) -> List[Dict[str, Union[int, str]]]:
        """
        Fetch enrollment data from the SQL database.

        :param course: Optional. If provided, fetch enrollments for this specific course.
        :return: List of dictionaries containing enrollment data.
        """
        query = f"SELECT {', '.join(self.fields)} FROM {self.enrollment_table}"
        if config.debug:
            print(f"Query: {query}")
        params = []

        if course:
            query += " WHERE shortname = ?"
            params.append(course)

        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [column[0] for column in cursor.description]
            data = cursor.fetchall()

        enrollments = [dict(zip(columns, row)) for row in data]

        # Convert role names to Moodle standard names if necessary
        role_mapping = {
            'student': 'student',
            'instructor': 'editingteacher',
            # Add more mappings as needed
        }

        for enrollment in enrollments:
            if 'role' in enrollment:
                enrollment['role'] = role_mapping.get(enrollment['role'].lower(), enrollment['role'])

        return enrollments

    def get_course_shortnames_for_sync(self) -> Set:
        """
        Return a set of course shortnames that should be synchronized.
        :return: Set of course shortnames.
        """
        query = f"SELECT distinct shortname FROM {self.enrollment_table}"

        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            data = cursor.fetchall()

        return {row[0] for row in data}



class MoodleMSSQLUserProvider(MoodleUserProvider):

    def __init__(self, connection_string: str, user_table: str = None):
        """

        :param connection_string: The PYODBC connection string for the database
        :param enrollment_table: The table from which to pull enrollments
        :param user_table: the table from which to pull users.  None if no sync users.
        """
        super().__init__()
        self.connection_string = connection_string
        self.user_table = user_table

    def get_user(self, email_username_or_id):
        """
        Fetch user data from the sql database.
        :param email_username_or_id:
        :return:
        """
        # see if email_username_or_id is an email, username, or numeric ID
        if '@' in email_username_or_id:
            field = 'email'
        elif email_username_or_id.isdigit():
            field = 'id'
            if 'id' not in self.fields:  # if we are doing the ID thing, let's do it!
                self.fields.append('id')
        else:
            field = 'username'
        query = f"SELECT {', '.join(self.user_fields)} FROM {self.user_table} WHERE {field} =  ?"
        params = [email_username_or_id]

        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [column[0] for column in cursor.description]
            data = cursor.fetchall()

        users = [dict(zip(columns, row)) for row in data]
        user = None if not users else users[0]
        return user

    def get_all_users(self) -> List[Dict]:
        """
        Fetch all users from the sql database.
        :return:
        """
        query = f"SELECT {', '.join(self.fields)} FROM {self.user_table}"

        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            data = cursor.fetchall()

        users = [dict(zip(columns, row)) for row in data]
        return users


