# file: moodle_sync/course.py

from typing import List, Dict, Callable, Union

from moodle_sync.config import config
from moodle_sync.logger import logger


class MoodleCourseProvider:
    """
    This is meant to be a base class.

    Provide a list of courses from a moodle instance or data provider, update courses, and create them.
    Override this class to
      pull / push course and category lists directly from Moodle, or from a local database or spreadsheet.


    The information returned and consumed by the provider is a list of dictionaries, where
        each dictionary entry is has columns and values, and the columns represent the columns
        from the Moodle course table that need to be kept up to date from the source.

    The data formats and field names for the dict
    should exactly match the data format to/from Moodle and the moodle field name.
    You can do field naming and type conversion on the provider end when you override.
    """

    r"""{  # This is what gets returned for a course
                        'id': 1901, 
                        'shortname':  'HIS--NNNN-F00, 2019-FA ', 
                        'categoryid': 13, 
                        'categorysortorder': 340251, 
                        'fullname': 'Senior Seminar:Hist/Pol.Sci (HIS--NNNN-F00, 2019-FA) ', 
                        'displayname': 'Senior Seminar:Hist/Pol.Sci (HIS--NNNN-F00, 2019-FA) ', 
                        'idnumber': 'HIS--NNNN_2019-FA_F00', 
                        'summary': 'This course is a senior seminar', 
                        'summaryformat': 1, 
                        'format': 'weeks', 
                        'showgrades': 1, 
                        'newsitems': 5, 
                        'startdate': 1566792000, 
                        'enddate': 1576476000, 
                        'numsections': 16, 
                        'maxbytes': 20971520, 
                        'showreports': 0, 
                        'visible': 0, 
                        'hiddensections': 0, 
                        'groupmode': 0, 
                        'groupmodeforce': 0, 
                        'defaultgroupingid': 0, 
                        'timecreated': 1501277513, 
                        'timemodified': 1566920251, 
                        'enablecompletion': 0, 
                        'completionnotify': 0, 
                        'lang': '', 
                        'forcetheme': '', 
                        'courseformatoptions': [{'name': 'hiddensections', 'value': 0}, {'name': 'coursedisplay', 'value': 0}, {'name': 'automaticenddate', 'value': 0}], 
                        'showactivitydates': False, 
                        'showcompletionconditions': None
                        }
                """

    fields = [
        # these are the columns that will be pushed to moodle for a course when a new course is created
        # All providers must return these dict keys with the proper types!  up to providers to cast to the right type.
        'shortname',
        'idnumber',  # a string.
        'fullname',
        'categoryid',  # integer category ID
        'summary',  # HTML text.  summaryformat may need to be 1.
        'startdate',  # dates are unix timestamp epoch ints
        'enddate',
        'course_summary',
        'format',  # weeks, topics, social, site, etc.
        'showgrades',
        'numsections',  # typically 16
        'visible',
        # available courseformatoptions :  ['hiddensections', 'coursedisplay', 'automaticenddate']
        # 'automaticenddate',   Add these after you have your instance if needed.

    ]

    fields_to_update = [
        # these values will be updated in moodle if they differ from the Source DB value.
        # these values are MOODLE course fields - a subset of the keys in the dictionary above.
        'fullname', 'startdate', 'enddate', 'categoryid',
    ]

    def __init__(self):
        self.courses = None
        self
        pass

    def get_category(self, name_or_id: Union[str, int]) -> Union[int, None]:
        raise NotImplementedError("No category getter provided.")

    def create_category(self, category_name, category_parent_name: Union[str, None] = None) -> int:
        raise NotImplementedError("No category creator provided.")

    def get_courses(self, field: Union[str, None] = None, value: Union[str, None] = None) -> List[Dict]:
        """
        Return a list of dictionaries of courses from a datasource (Moodle, CSV, Database - whatever).

        The keys in each dictionary should either match the source keys or the Moodle keys from CourseSync
        You can call this function at the END of your derived class if you want some sanity checks
        after setting the self.courses in your own get_courses implementation.
        @param field: str:  field to filter on.
        @param value: str:  value to filter on.
        :return: List[dict]:  list of courses with fields and values.
        """
        # some basic sanity checks
        if self.courses is None:
            raise NotImplementedError("No courses provided.")
        else:
            courses = self.courses
        assert isinstance(courses, List), "retriever must return a list of dicts."
        for course in courses:
            assert isinstance(course, Dict), "retriever must return a list of dicts"

        if len(courses) > 0:
            assert isinstance(courses[0], Dict), "retriever must return a list of dicts"
            for column in self.fields:
                assert column in courses[0], f"retriever must return a list of dicts with the column: {column}"
        return courses

    def get_course(self, shortname_or_id: Union[str, int]) -> Union[dict, None]:
        raise NotImplementedError("No course getter provided.")

    def create_course(self, course: Dict):
        raise NotImplementedError("No creator provided.")

    def update_course(self, course: Dict):
        raise NotImplementedError("No updater provided.")


class CourseSync:

    def __init__(self, target: MoodleCourseProvider, source: MoodleCourseProvider,
                 course_key='shortname',
                 category_name_key='categoryname',
                 category_parent_name_key=None):
        """
        idnumber and shortname are common values for the course_key
        :param target:
        :param source:
        :param course_key: string to identify the field to be used as the course primary key.
        :param category_name_key: string to identify the field for the category name in the source course data.
        :param category_parent_name_key: string to identify the field for the category name in the source course data.

        """
        self.target = target
        self.source = source
        self.course_key = course_key
        self.category_name_key = category_name_key
        self.category_parent_name_key = category_parent_name_key

    def course_update_needed(self, moodle_course, source_course) -> bool:
        """
        Compare the source course and the moodle course to see if the moodle course needs to be updated.
        :param moodle_course: dict: course from moodle
        :param source_course: dict: course from source
        :return: bool: True if the moodle course needs to be updated.
        """
        def htmldecode(s):
            # hacky get it done fast replace HTML things.  Just used to detect changes.
            if type(s) is str:
                return s.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"').replace(
                '&#039;', "'")
            return s

        diffs = ""
        for field in self.target.fields_to_update:
            moodle_course_field_value = htmldecode(moodle_course.get(field, ''))
            source_course_field_value = htmldecode(source_course.get(field, ''))
            if field not in moodle_course:
                logger.debug(f"Field {field} not in moodle course.")
            elif field not in source_course:
                logger.debug(f"Field {field} not in source course.")
            elif moodle_course_field_value != source_course_field_value:
                # if we don't care about the end date, don't update it if it changes.
                if field == 'enddate' and 'automaticenddate' not in self.target.fields_to_update \
                        and moodle_course.get('automaticenddate') == 1:
                    # skip updating enddate if automaticenddate is off and automaticenddate not in the fields to update.
                    pass
                else:
                    diffs = diffs + f"   moodle {field} was {moodle_course[field]} now {source_course[field]}\n"
        if diffs:
            logger.debug("Differences found: ", diffs)
        return diffs != ""

    def get_moodle_category_from_course(self, course, create=True):
        """
        Look up the Moodle category ID for the given course.
        :param course: dict: course from the source
        :param create: bool: create the category if it does not exist.
        :return: int: category ID  or None if not found.
        """

        try:
            category_id = self.target.get_category(course[self.category_name_key])
        except ValueError as e:
            logger.debug(f"Category Not Found for course {course['shortname']} {course[self.category_name_key]}: {e}")
            category_id = None
        if category_id is None and create:
            category_id = self.target.create_category(course[self.category_name_key],
                                                      course.get(self.category_parent_name_key))
        return category_id

    def sync_to_moodle(self, fetch='one'):
        """
        Sync courses from the source provider to Moodle.
        :param fetch: one or all.  If all, then get all courses from moodle, otherwise each that matchs the source.
        :return:
        """
        source_courses = self.source.get_courses()
        if fetch == 'all':
            moodle_courses = self.target.get_courses()
        elif fetch == 'one':
            moodle_courses = []  # evaluates to False
        else:
            raise ValueError("fetch must be one or all (lowercase).")

        logger.info(f"Found  {len(source_courses)} courses in source.")
        cnt_created, cnt_updated, cnt_skipped, cnt_error = 0, 0, 0, 0
        for course in source_courses:
            action = 'what is it we are doing?'
            if True: #try:  # keep going after individual failures.
                action = 'get category'
                category_id = self.get_moodle_category_from_course(course)
                course['categoryid'] = category_id

                # find moodle course
                if fetch == 'all':
                    moodle_course = next((c for c in moodle_courses if c[self.course_key] == course[self.course_key]),
                                         None)
                else:
                    logger.debug("Searching for ", course[self.course_key])
                    action = 'get course'
                    moodle_course = self.target.get_course(course[self.course_key])

                # now create or update the course.
                if moodle_course is None:
                    logger.info("Creating ", course[self.course_key])
                    action = 'create course'
                    self.target.create_course(course)
                    cnt_created += 1
                    #print("course.py sync_to_moodle BREAKING!  Created a course!  Check it out.")
                    #break
                else:
                    action = 'determine update needed'
                    if self.course_update_needed(moodle_course, course):
                        logger.info("Updating ", course[self.course_key])
                        action = 'update course'
                        self.target.update_course(course)
                        cnt_updated += 1
                        #if course['enddate'] < 1730338814:
                        #    print("course.py sync_to_moodle BREAKING!  Updated a semester course  Check it out.", moodle_course['id'])
                        #    break
                    else:
                        logger.info("No update needed for ", course[self.course_key])
                        cnt_skipped += 1
            try:
                pass
            except Exception as e:
                logger.error(f"Error {action} ", course[self.course_key], type(e).__name__, str(e))
                cnt_error += 1
                print("ERROR on something!  BREAK")
                break
            pass  # end for course in source_courses
        logger.info(f"Created {cnt_created}, Updated {cnt_updated}, Skipped {cnt_skipped}, Errors {cnt_error}")
        return
