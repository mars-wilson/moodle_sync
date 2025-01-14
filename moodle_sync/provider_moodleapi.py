# file: moodle_sync/provider_moodleapi.py

import requests, json, re
from typing import Union, Any, Dict, List

from moodle_sync.config import config
from moodle_sync.logger import logger
from moodle_sync.course import MoodleCourseProvider
from moodle_sync.enrolment import MoodleEnrolmentProvider
from moodle_sync.user import MoodleUserProvider

"""
You can see all available API functions here:

https://YOURSITE.example.com/admin/webservice/documentation.php

The base class defines roles. Note that the Webservice Get Roles function is required to get roles
and this is not a standard Moodle API function.  You can redefine your roles manually if you wish.

"""

from requests.adapters import HTTPAdapter
import urllib3.util.connection
import urllib3.connection
import requests
from typing import Optional


class CustomDNSAdapter(HTTPAdapter):
    def __init__(self, ip_address: Optional[str] = None, *args, **kwargs):
        self.ip_address = ip_address
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        # Save the original create_connection function
        self._original_create_connection = urllib3.connection.HTTPConnection.create_connection

        if self.ip_address is not None:
            # Override the create_connection with our custom one only if ip_address is specified
            def patched_create_connection(address, *args, **kwargs):
                host, port = address
                return urllib3.util.connection.create_connection((self.ip_address, port), *args, **kwargs)

            urllib3.connection.HTTPConnection.create_connection = patched_create_connection

        # Call parent's init_poolmanager
        super().init_poolmanager(*args, **kwargs)

    def close(self):
        # Restore the original create_connection when done
        if self.ip_address is not None:
            urllib3.connection.HTTPConnection.create_connection = self._original_create_connection
        super().close()


class CustomDNSSession:
    def __init__(self, ip_address: Optional[str] = None):
        """
        Initialize a session with optional custom DNS resolution

        Args:
            ip_address (str, optional): The IP address to connect to. If None, uses regular DNS resolution.
        """
        self.session = requests.Session()
        self.adapter = CustomDNSAdapter(ip_address)
        self.session.mount('http://', self.adapter)
        self.session.mount('https://', self.adapter)

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Clean up resources"""
        self.adapter.close()
        self.session.close()

    def request(self, method: str, url: str, **kwargs):
        """
        Make a request using the session

        Args:
            method (str): HTTP method (get, put, post, etc.)
            url (str): The URL to request
            **kwargs: Additional arguments to pass to requests
        """
        return self.session.request(method, url, **kwargs)

    def get(self, url: str, **kwargs):
        """Convenience method for GET requests"""
        return self.request('get', url, **kwargs)

    def put(self, url: str, **kwargs):
        """Convenience method for PUT requests"""
        return self.request('put', url, **kwargs)

    def post(self, url: str, **kwargs):
        """Convenience method for POST requests"""
        return self.request('post', url, **kwargs)





class MoodleAPI:
    """
    Provide a way to invoke the Moodle API and parse out the results.

    This class creates a singleton borgy thingy sort of - one instance for each moodle site -
    so that cached data can remain consistent for two different moodle sites if you ever wanted to do that.

    Note:  the first parameter is called "self_api" to facilitate some console debugging and functionality.
    This of course is weird!  Just take a moment to smile and be grateful for your next breath.
    The above is very important.

    Also important:  You can pass either requests.post or requests.get to the execute function.
    If dryrun is true in config, requests.post won't do anything and no API call will be made
    The execute function has a dryrun_result variable to use for debugging purposes in that case.
    """

    _instances = {}  # one singleton instance for each Moodle site.

    roles = [
        {"id": 0, "name": "None", "shortname": "none", "sortorder": 0, "archetype": "",
         "description": "No Role.",
         },
        {   "id": 1, "name": "Manager", "shortname": "manager", "sortorder": 1, "archetype": "manager",
            "description": "Managers can access courses and modify them.",
        },
        {   "id": 2, "name": "Course creator", "shortname": "coursecreator",  "sortorder": 2, "archetype": "coursecreator",
            "description": "Course creators can create new courses.",
        },
        {    "id": 3, "name": "Teacher",     "shortname": "editingteacher",  "sortorder": 3, "archetype": "teacher",
            "description": "Teachers can manage and grade course activities.",
        },
        {   "id": 4, "name": "Non-editing teacher", "shortname": "teacher", "sortorder": 4, "archetype": "teacher",
            "description": "Non-editing teachers can grade in courses but not edit them.",
        },
        {   "id": 5, "name": "Student",  "shortname": "student",  "sortorder": 5,  "archetype": "student",
            "description": "Students can participate in course activities.",
        },
        {    "id": 6, "name": "Guest", "shortname": "guest", "sortorder": 6, "archetype": "guest",
            "description": "Guests can view courses but not participate.",
        },
        {   "id": 7, "name": "Authenticated user", "shortname": "user", "sortorder": 7, "archetype": "user",
            "description": "All logged-in users have this role.",
        },
        {   "id": 8, "name": "Authenticated user on the front page", "shortname": "frontpage", "sortorder": 8, "archetype": "frontpage",
            "description": "A logged-in user role for the front page only.",
        }
    ]

    @staticmethod
    def __new__(cls, site, api_key):
        """
        Implement a singleton pattern for the MoodleAPI class.
        :param site:
        :param api_key:
        """
        if site not in cls._instances:
            cls._instances[site] = super(MoodleAPI, cls).__new__(cls)
        else:
            pass

        return cls._instances[site]



    def __init__(self, site, api_key):
        if hasattr(self, '__initialized'):
            # already initialized.  But allow updates to the api_key
            self.api_key = api_key
            return
        self.__initialized = True
        self.site = site
        self.endpoint = f'https://{site}/webservice/rest/server.php'
        self.api_key = api_key

        self.user_cache = {}  # cache user ids
        self.course_cache = {}  # cache course ids
        self.course_contexts = {} # which courses have which context IDs.
        self.webservice_get_roles_installed = False

        # useful for some debugging action
        self.last_api_details = {}

    def execute(self_api, requests_func, params, dryrun_result=None) -> Any:
        """
        Execute a requests get or post or something like that.
        Convention is that post makes changes.  If debug, then that will be logged but not executed.
        :param requests_func:  requests.get,  requests.post
        :param params: the parameters to post.
        :param dryrun_result:  If provided, return this instead of invoking the actual result.
        :return: data or raises an exception if an error.

        """
        params['wstoken'] = self_api.api_key
        params['moodlewsrestformat'] = 'json'

        early_result = None

        if (requests_func != requests.get and config.dryrun):
            logger.debug(f"DRYRUN mode: API call details: ", params)
            early_result = dryrun_result

        if early_result is not None:
            # return early response
            return early_result

        # store actual API details for debugging.
        self_api.last_api_details['params'] = params
        self_api.last_api_details['func'] = requests_func
        response = requests_func(self_api.endpoint, params=params)
        self_api.last_api_details['response'] = response

        if response.status_code != 200:
            logger.error(f"API call failed: Status code {response.status_code}", 'Response:', response.text)
            raise Exception(f"Error occurred: {response.status_code}, {response.text}")

        data = json.loads(response.text)
        self_api.last_api_details['data'] = data

        if data is None and not response.ok:
            logger.error(f"API call failed: Response Not OK: {response.reason}")
            raise requests.exceptions.HTTPError(f"Error occurred: Response Not OK: {response.reason}")
        elif type(data) is dict and 'exception' in data:
            logger.error(f"API call failed: {data['exception']} {data.get('debuginfo', '')}")
            raise requests.exceptions.HTTPError(f"Error occurred: {data['exception']} {data.get('debuginfo', '')}")

        if config.debug:
            logger.debug(f"API call successful: {requests_func.__name__} {params.get('wsfunction')}")
        return data


    def  get_user_id(self, email_username_or_id: Union[str, int]) -> Union[int, None]:
        user = self.get_user(email_username_or_id)
        user_id = user['id'] if user else None
        return user_id

    def get_user(self, email_username_or_id: Union[str, int]) -> Union[int, None]:
        """
        Get the user id for the given email or user ID.  Return None if not found.  Cache the result.
        :param email_or_id: str or int: the email address or user ID.  Note there is no username search
        :return: int: the user id, or None if the user doesn't exist
        """
        # Check if the result is already in the cache
        if email_username_or_id in self.user_cache:
            return self.user_cache[email_username_or_id]

        # Determine whether we're dealing with a username or user ID
        if isinstance(email_username_or_id, int) or email_username_or_id.isdigit():
            key = 'id'
            value = str(email_username_or_id)
        elif '@' in email_username_or_id:
            key = 'email'
            value = email_username_or_id
        else:
            key = 'username'
            value = email_username_or_id


        """ Here is a user that the below params pulls:
        {
        'id': 3, 'username': 'mlandis', 'firstname': 'Mars', 'lastname': 'Landis', 'fullname': 'Mars Landis', 
        'email': 'mlandis@warren-wilson.edu', 'department': '', 'institution': '253NNN', 
        'idnumber': '269A918F-C1D2-4466-B128-asdfasdfasdf', 'firstaccess': 1495797486, 'lastaccess': 1719747709, 
        'auth': 'ldap', 'suspended': False, 'confirmed': True, 'lang': 'en_us', 'theme': '', 'timezone': '99', 
        'mailformat': 1, 'description': '', 'descriptionformat': 1, 'city': 'Not Available', 'country': 'US', 
        'profileimageurlsmall': 'https://moodletest.warren-wilson.edu/pluginfile.php/NNNN/user/icon/boost/f2?rev=70502', 
        'profileimageurl': 'https://moodletest.warren-wilson.edu/pluginfile.php/NNNN/user/icon/boost/f1?rev=70502'
        }
    
        """

        params = {
            'wsfunction': 'core_user_get_users_by_field',
            'field': key,
            'values[0]': value
        }
        data = self.execute(requests.get, params)

        if data and len(data) > 0:
            user = data[0]
            # Cache both the email/username and user ID
            self.user_cache[data[0]['email']]  = user
            self.user_cache[data[0]['username']] = user
            self.user_cache[data[0]['id']] = user
            return user

        # If no user found, cache the negative result to avoid future API calls
        self.user_cache[email_username_or_id] = None
        return None

    def create_user(self, username: str, email: str, firstname: str, lastname: str, auth: str, password: str):

        params = {
            'wsfunction': 'core_user_create_users',
            'users[0][username]': username,
            'users[0][auth]': auth,
            'users[0][email]': email,
            'users[0][firstname]': firstname,
            'users[0][lastname]': lastname,
            'users[0][password]': password
        }
        result = self.execute(requests.post, params)
        return result[0].get('id') if type(result) is list and type(result[0] is dict) else None

    def get_category(self_api, name_or_id: Union[str, int]) -> int:
        """
        Look up the category by name or ID.
        :param name_or_id:
        :return:
        :raise Raises ValueError if ategory not found
        """
        assert type(name_or_id) is str or type(name_or_id) is int, "name_or_id must be category name (str) or id (int)"
        params = {
            'wsfunction': 'core_course_get_categories',
            'criteria[0][key]': 'name' if type(name_or_id) is str else 'id',  # Adjusted format for criteria
            'criteria[0][value]': name_or_id  # Replace with the actual category name
        }

        # requst_func = requests.get, params=params  # response = requests.get(self.endpoint, params=params)
        data = self_api.execute(requests.get, params=params)

        if len(data) > 0:
            result = data[0]["id"]
        else:
            logger.debug(f"Category not found: {name_or_id}")
            raise ValueError(f"Category not found: {name_or_id}")
        return result


    def get_role_id(self, rolename_or_id: Union[str, int]) -> Union[int, None]:
        """
        Get the role id for the given role name or role ID.

        :param rolename_or_id: str or int: the role name or role ID
        :return: int: the role id, or None if the role doesn't exist
        """
        # Check if the result is already in the defined roles
        if str(rolename_or_id) in [str(role['id']) for role in self.roles]:
            return int(rolename_or_id)
        if str(rolename_or_id) in [role['shortname'] for role in self.roles]:
            return next(role['id'] for role in self.roles if role['shortname'] == rolename_or_id)

        if not self.webservice_get_roles_installed:
            return None

        # the below functionality will throw an exception if the webservice get roles plugin is not installed.
        # Determine whether we're dealing with a role name or role ID
        if isinstance(rolename_or_id, int) or rolename_or_id.isdigit():
            key = 'id'
            value = str(rolename_or_id)
        else:
            key = 'shortname'
            value = rolename_or_id

        params = {
            'wsfunction': 'core_role_get_roles',
            'moodlewsrestformat': 'json',
        }

        try:
            data = self.execute(requests.get, params)
        except requests.exceptions.HTTPError as e:
            if 'dml_missing_record_exception' in str(e):
                raise RuntimeError("get_role_id requires the webservice get roles plugin.  Or you can define your roles manually.  ")
        if data:
            for role in data:
                if str(role[key]) == value:
                    role_id = role['id']
                    # Cache both the role name and role ID
                    self.roles.append(data)
                    return role_id

        # If no role found, cache the negative result to avoid future API calls
        self.roles.append(
            {'id': None, 'shortname': rolename_or_id, 'name': 'Unknown Role', 'sortorder': -1, 'archetype': None,
              'description': f'Role {rolename_or_id} not found',})
        return None

    def define_roles(self, roles: List[dict], append=True ) -> None:
        """
        In lieu of using the webservice get roles plugin, you can define your roles here. based on roles from
        your Moodle Admin Interface.

        :param roles: a list of dicts with keys: id, shortname, name, sortorder, archetype, description
        :param append: bool: append to the existing roles or replace them.
        :return:
        """
        assert all(role.keys() == ['id', 'shortname', 'name'] for role in roles), "Role must have id, shortname, name"

        if not append:
            self.roles = roles
        self.roles.extend(roles)

    def get_course_id(self, shortname_or_id: Union[str, int]) -> Union[int, None]:
        """
        Get the course id for the given course shortname or course ID.
        :param shortname_or_id: str or int: the course shortname or course ID
        :return: int: the course id, or None if the course doesn't exist
        """
        # Check if the result is already in the cache
        if shortname_or_id in self.course_cache:
            return self.course_cache[shortname_or_id]

        # Determine whether we're dealing with a shortname or course ID
        if isinstance(shortname_or_id, int) or shortname_or_id.isdigit():
            field = 'id'
            value = str(shortname_or_id)
        else:
            field = 'shortname'
            value = shortname_or_id

        params = {
            'wsfunction': 'core_course_get_courses_by_field',
            'moodlewsrestformat': 'json',
            'field': field,
            'value': value
        }

        data = self.execute(requests.get, params)

        if data and 'courses' in data and len(data['courses']) > 0:
            course = data['courses'][0]
            course_id = course['id']
            # Cache both the shortname and course ID
            self.course_cache[course['shortname']] = course_id
            self.course_cache[course_id] = course_id
            return course_id

        # If no course found, cache the negative result to avoid future API calls
        self.course_cache[shortname_or_id] = None
        return None


    def _refresh_contextid_for_courseid(self_api):
        """
        This API call require a plugin.
        But it's not currently used anyway - just came up with research so it's here.

        https://moodle.org/plugins/local_getcontexts

        :return: None
        """
        # Define the API function and parameters
        params = {
            'wsfunction': 'core_context_get_contexts',
            'moodlewsrestformat': 'json',
        }

        # Make the API request to get contexts
        contexts = self_api.execute(requests.get, params)

        for context in contexts:
            self_api.course_contexts[context['instanceid']] = context['id']


    def get_contextid_for_courseid(self_api, course_id: int) -> Union[int, None]:
        """
        requires a plugin.  See above.  Unused.
        Get the context id for the given course id.
        :param course_id: int: the course id
        :return: int: the context id, or None if the course doesn't exist
        """
        context = self_api.course_contexts.get(course_id)
        if not context:
            # try again.  Look em up in case courses changed.
            self_api._refresh_contextid_for_courseid()
        context = self_api.course_contexts.get(course_id)
        return context



class MoodleAPICourseProvider(MoodleCourseProvider):
    """

    SEE https://docs.moodle.org/dev/Web_service_API_functions

    For docs on specific functions search for them in this php:
    https://github.com/moodle/moodle/blob/master/course/externallib.php
    https://github.com/moodle/moodle/blob/MOODLE_37_STABLE/course/externallib.php

    """

    templates = [
        # You wil need to customize this for your own values.
        # a list of tuples - first is a regex pattern to match the shortname, second in tuple is a Moodle Course ID.
        # (Shortname Regex - moodle Course ID)
        # (r'^HIS.*', 1901),   # match history courses and use template course 1901
        # if the key is a string, the template course will be looked up by course shortname.
        ('', 2)  # The final template will be used if there are no matches.  Defaults to the first course in Moodle.
    ]

    # these fields need to be mapped from a dict and flattened into a course when getting a course,
    # and then mapped back to a dict when updating a course.
    courseformatoptions_fields = ['hiddensections', 'coursedisplay', 'automaticenddate']

    def __init__(self, site, api_key, templates=None):
        super().__init__()
        self.api = MoodleAPI(site, api_key)

        # promote templates to an instance variable.
        if templates is None:
            templates = self.templates
        self.templates = templates
        self.get_all_courses = True  # preference to attempt to load all the courses with this provider.

    def _get_template(self, shortname: str) -> int:
        """
        Look for a template that matches the shortname.  If none found, return the last template.
        :param shortname:
        :return: int - the course ID of the template to use.
        """
        result = None
        for pattern, course_id in self.templates:
            if re.match(pattern, shortname):
                result = course_id
        if not result:
            result = self.templates[-1][1]
        if type(result) is str:
            result = self.api.get_course_id(result)
        return result

    def _extract_courseformatoptions(self, course: dict):
        """
        This function mutes the course dict.
        It returns a dict formatted for the courseformatoptions field for updating Moodle
        :param course:
        :return: a dict of properly formatted params
        """
        # convert course format options to proper format
        # 'courseformatoptions': [{'name': 'hiddensections', 'value': 0}, {'name': 'coursedisplay', 'value': 0}, {'name': 'automaticenddate', 'value': 0}],
        #         params[f'courses[{i}][courseformatoptions][0][name]'] = 'hiddensections'
        #         params[f'courses[{i}][courseformatoptions][0][value]'] = hidden_sections_value
        courseformatoptions, counter = {}, 0
        # self.courseformatoptions_fields = ['hiddensections', 'coursedisplay', 'automaticenddate']
        for field in self.courseformatoptions_fields:
            if field in course:
                courseformatoptions[f'courses[{counter}][courseformatoptions][0][name]'] = field
                courseformatoptions[f'courses[{counter}][courseformatoptions][0][value]'] = course[field]
                del course[field]
                counter += 1
        return courseformatoptions

    def _flatten_courseformatoptions(self, course: dict):
        """
        This function mutates the course dict.
        It removes courseformatoptions from the course and flattens those values into the course dict itself
        :param course:
        :return: a dict of properly formatted params
        """
        # convert course format options to proper format
        # 'courseformatoptions': [{'name': 'hiddensections', 'value': 0}, {'name': 'coursedisplay', 'value': 0}, {'name': 'automaticenddate', 'value': 0}],
        #         params[f'courses[{i}][courseformatoptions][0][name]'] = 'hiddensections'
        #         params[f'courses[{i}][courseformatoptions][0][value]'] = hidden_sections_value
        courseformatoptions = course.get('courseformatoptions', [])
        for name_value_pair in courseformatoptions:
            course[name_value_pair['name']] = name_value_pair['value']
        if course.get('courseformatoptions', None) is not None:
            del course['courseformatoptions']
        return course

    def get_course(self, shortname_or_id: Union[str, int]) -> Union[dict, None]:
        """
        Return the course  for the  given shortname or course id
        """
        field = 'shortname' if type(shortname_or_id) is str else 'id'
        courses_matching = self.get_courses(field=field, value=shortname_or_id)
        if courses_matching:
            print(courses_matching[0])
            return courses_matching[0]
        return None

    def get_courses(self, field=None, value=None):
        params = {
            'wsfunction': 'core_course_get_courses',
        }

        if field:
            # use the newer API call to get by field
            params['field'] = field
            params['value'] = value
            params['wsfunction'] = 'core_course_get_courses_by_field'
        # else data will be just a list of all courses.

        logger.debug("Fetching courses: " + str(params))

        # this works. but the data is a dict with 'courses' if you put in a field and value. Otherwise a list:
        """
        params = {'wstoken': self.api_key,
                  'wsfunction': 'core_course_get_courses_by_field',
                  'moodlewsrestformat': 'json',
                  'field': 'category',
                  'value': '23'}
        # data will be a dict with key "courses" and "warnings"
        
        Note that if you pass in field startdate, the value will be used to fetch courses on or after that date.
        """
        # request_func, request_params = requests.get, params     # response = requests.get(self.endpoint, params=params)
        data = self.api.execute(requests.get, params=params)
        courses = data if isinstance(data, list) else data.get('courses', [])
        for course in courses:
            self._flatten_courseformatoptions(course)
        logger.debug(f"Retrieved {len(courses)} courses")
        return courses

    def create_course(self, course: dict) -> Union[int, None]:
        """
        Create the course from a template. Use default template if not found.

        :param course: a dict of course values
        :return: course ID if course was created else None
        :raises: ValueError if categoryid not found (from the course dict)
        """

        # make sure the course does not exist
        existing_course = self.get_course(course['shortname'])
        if existing_course:
            logger.error(f"Course already exists with shortname: {course['shortname']}")
            raise ValueError(f"Course already exists with shortname: {course['shortname']}")

        # check to make sure the category exists.  Would raise ValueError.
        course['categoryid'] = self.api.get_category(course['categoryid'])

        course_basics = {
            'fullname': course['fullname'],  # Required: The full name of the course
            'shortname': course['shortname'],  # Required: The short name of the course
            'categoryid': course['categoryid'],  # Required: The category id the course belongs to
        }
        params = {
            'wsfunction': 'core_course_duplicate_course',
            'courseid': self._get_template(course['shortname']),
        }
        params.update(course_basics)
        # requests_func = requests.post, params=params
        data = self.api.execute(requests.post, params, dryrun_result={'id': -999} if config.dryrun else None)

        new_course_id = data['id'] if data else None
        if config.dryrun:
            logger.info(f"Dryrun Mode - Skip Update id {new_course_id}: {course_basics} ")
            return

        if new_course_id:
            # filter the course values for just Moodle fields, and then update_course
            course_update_values = {k: v for k, v in course.items() if k in self.fields_to_update}
            self.update_course(course, force_all_fields=True, course_id=new_course_id)
        else:
            logger.error(f"Course not created: {course['shortname']}")
        return new_course_id

    def update_course(self, course: dict, force_all_fields=False, course_id: Union[int, None] = None):
        """
        Update the course with the dict values.
        Only update fields in the provider fields_to_update list unless force_all_fields is on.
            Then update all that can be updated that are in fields
            Skip fields not in the provider fields list

        :param course: course dict ready for Moodle
        :param force_all_fields: bool - Force all  fields to update else just the ones in fields_to_update
        :param course_id: int - the course id to update.  If None, it will be looked up by shortname
        :return: None.  Will raise exception if fail
        """
        filtered_fields = {k: v for k, v in course.items()
                           if k in self.fields_to_update or (force_all_fields and k in self.fields)}

        if config.dryrun:
            existing_course = {'id': -999}
        else:
            existing_course = self.get_course(course['shortname'] if course_id is None else course_id)
        if not existing_course:
            logger.error(f"Existing Course not found: {course['shortname']}")
            raise ValueError(f"Existing Course not found: {course['shortname']}")

        # The update courses api requires the id, which is why look it up above. It's usually not in the course dict.
        filtered_fields['id'] = existing_course['id']
        course_format_options = self._extract_courseformatoptions(filtered_fields)

        # Flatten the array for the params.  This is the way Moodle wants it.
        # Note  course format options must be first removed, so after, update courses_param with course_format_options
        courses_param = {f'courses[0][{key}]': value for key, value in filtered_fields.items()}
        courses_param.update(course_format_options)

        params = {
            'wsfunction': 'core_course_update_courses',
            **courses_param  # Note you could just do params.update(courses_param) as well.
        }

        #logger.debug(f"Updating Course: {course['shortname']} with: \n    ", params)
        _data = self.api.execute(requests.post, params, dryrun_result=course if config.dryrun else None)
        logger.debug(f"Course {existing_course['id']} Updated: {course['shortname']} with:  \n   ", params)
        return


    def get_category(self, name_or_id: Union[str, int]) -> int:
        """
        Look up the category by name or ID.
        :param name_or_id:
        :return:
        :raise Raises ValueError if ategory not found
        """
        return self.api.get_category(name_or_id)

    def create_category(self, category_name, category_parent_name: Union[str, None] = None) -> int:
        """
        Create a category with the given name and parent name.
        :param category_name: str: the name of the category
        :param category_parent_name: str: the name of the parent category
        :return: int: the category id
        """
        parent_id = self.get_category(category_parent_name) if category_parent_name else None

        params = {
            'wsfunction': 'core_course_create_categories',
            'categories[0][name]': category_name,
        }
        if category_parent_name:
            params['categories[0][parent]'] = parent_id
        data = self.api.execute(requests.post, params, dryrun_result={'id': -99} if config.dryrun else None)
        logger.debug(f"Category Created: {category_name} id {data[0]['id']}")
        return data[0]['id']


class MoodleAPIEnrolmentProvider(MoodleEnrolmentProvider):

    def __init__(self, site, api_key):
        super().__init__()
        self.api = MoodleAPI(site, api_key)
        self.roles_to_sync = ['student', 'editingteacher']

    def get_role_id(self, role: str) -> Union[None, int]:
        """
        Return the role id for a role name.
        :param role: str: The role name.
        :return: int: The role id.
        """
        return self.api.get_role_id(rolename_or_id=role)


    def get_user_id(self, email_username_or_id: Union[str, int]) -> Union[int, None]:
        """
        Return the user id for the given email or user ID.
        :param email_username_or_id: str or int: the email address or user ID
        :return: int: the user id, or None if the user doesn't exist
        """
        return self.api.get_user_id(email_username_or_id)

    def get_username(self, user_id: int) -> Union[None, str]:
        """
        Return the username for a user id.
        :param user_id: int: The user id.
        :return: str: The username.
        """
        user = self.api.get_user(user_id)
        return user['username'] if user else None

    def get_course_id(self, shortname: str) -> Union[None, int]:
        """
        Return the course id for a shortname.
        :param shortname: str: The shortname.
        :return: int: The course id.
        """
        return self.api.get_course_id(shortname)



    def get_enroled_users(self, course: Union[str, int]) -> List[Dict[str, int]]:
        """
        Return a list of users and roles for a course.
        @param course: int or str: the course id or shortname
        @return: list: A list of Dicts with user_id, course_id, and role_id
        """

        course_id = self.api.get_course_id(course)
        if course_id is None:
            raise ValueError(f"Course does not exist: {course}")

        params = {
            'wsfunction': 'core_enrol_get_enrolled_users',
            'courseid': course_id
        }

        data = self.api.execute(requests.get, params)
        # what comes back is a long list of this stuff:
        # [ {'id': 2454, 'fullname': 'Maryam Mirili', 'email': 'mmirili.f20@warren-wilson.edu',
        # 'firstaccess': 1590825777, 'lastaccess': 1715406683,
        # 'lastcourseaccess': 0, 'description': '', 'descriptionformat': 1, 'city': 'Not Available', 'country': 'AZ',
        # 'profileimageurlsmall': 'https://moodle3.warren-wilson.edu/theme/image.php/boost/core/1718055491/u/f2',
        # 'profileimageurl': 'https://moodle3.warren-wilson.edu/theme/image.php/boost/core/1718055491/u/f1',
        # 'roles': [{'roleid': 5, 'name': '', 'shortname': 'student', 'sortorder': 0}],
        # 'enrolledcourses': [{'id': 1994, 'fullname': 'Chemistry Placement Test', 'shortname': 'Chemistry Placement'},
        # {'id': 1996, 'fullname': 'Language Placement Test', 'shortname': 'Language Placement'},
        # {'id': 1995, 'fullname': 'Math Placement Tests', 'shortname': 'Math Placement'}}]
        # }, ...]
        # generate the return list based on the above data returned...
        if type(data) is list:
            enrolmnent_list = [
                {'user_id': user['id'], 'course_id': course_id, 'role_id': role['roleid'], 'role': role['shortname']}
                for user in data
                for role in user['roles'] if role['shortname'] in self.roles_to_sync
            ]
        else:
            enrolmnent_list = []
        # log the number of people in each role for roles_to_sync
        counts = []
        for role in self.roles_to_sync:
            num = len([enrol for enrol in enrolmnent_list if enrol['role'] == role])
            counts.append(f"{num} {role}s")
        logger.debug(f"Retrieved {len(enrolmnent_list)} enrolments for course {course_id}: {', '.join(counts)}")
        return enrolmnent_list

    def course_enrol_user(self, user: Union[int, str], course: Union[str, int], role: Union[str, int] = 'student') \
            -> Union[None, Dict[str, int]]:
        """
        Add a user to a course with a role.
        :param user: int,str: the user id
        :param course: int,str: the course id
        :param role: int,str: the role to add the user to the course
        :return: None or dict of the user with counts as 1
        """
        user_id = self.api.get_user_id(user)
        if user_id is None:
            logger.error(f"User does not exist: {user}")
            raise ValueError(f"User does not exist: {user}")

        course_id = self.api.get_course_id(course)  # performs a lookup
        if course_id is None:
            logger.error(f"Course does not exist: {course}")
            raise ValueError(f"Course does not exist: {course}")

        role_id = self.api.get_role_id(role)
        if role_id is None:
            logger.error(f"Role does not exist: {role}")
            raise ValueError(f"Role does not exist: {role}")

        params = {
            'wsfunction': 'enrol_manual_enrol_users',
            'enrolments[0][roleid]': role_id,
            'enrolments[0][userid]': user_id,
            'enrolments[0][courseid]': course_id
        }
        alreadythere = self._user_has_role_in_course(user_id, course_id, role_id)
        if alreadythere:
            logger.debug(f"User {user} already enrolled in role {role} in course {course_id}")
            return None
        data = self.api.execute(requests.post, params, dryrun_result='yes!')
        if data is None or data == 'yes!':  # The API returns None on success.
            logger.info(f"User {user_id} enrolled in role {role_id} in course {course_id}")
            return {"user_id": user_id, "course_id": course_id, "role_id": role_id,
                    "num_new_enrols": 1, "num_roles_added": 1}  # note the new_enrols is made up to indicate success.
        else:
            logger.error(f"Failed to add user {user} to course {course_id}. API response: {data}")
            raise Exception(f"Failed to add user {user} to course {course_id}. API response: {data}")
        pass

    def course_unenrol_user(self, user: Union[int, str], course: Union[int, str], role: Union[int, str]) \
            -> Union[None, Dict[str, int]]:
        """
        Unenrol a user from the given role in a course.
        This method requires a plugin.  https://moodle.org/plugins/local_getcontexts?lang=en_us
        This is really stupid that this function doesn't take a courseid.

        :param user: int or str: the user id or username
        :param course: int or str: the course id or shortname
        :param role: str or int: the role to remove the user from the course
        :return: Dict with user_id, course_id, role_id, and num_roles_deleted of the unenroled user or None if no change.
        :raises ValueError: if the user, course, or role does not exist
        :raises requests.exceptions.RequestException: if the API call fails
        """
        user_id = self.api.get_user_id(user)
        if user_id is None:
            logger.error(f"User does not exist: {user}")
            raise ValueError(f"User does not exist: {user}")

        course_id = self.api.get_course_id(course)
        if course_id is None:
            logger.error(f"Course does not exist: {course}")
            raise ValueError(f"Course does not exist: {course}")

        role_id = self.api.get_role_id(role)
        if role_id is None:
            logger.error(f"Role does not exist: {role}")
            raise ValueError(f"Role does not exist: {role}")

        params = {
            'wsfunction': 'core_role_unassign_roles',
            'unassignments[0][roleid]': role_id,
            'unassignments[0][userid]': user_id,
            'unassignments[0][contextlevel]': 'course', #50 is the context level for course.  A string.
            'unassignments[0][instanceid]': course_id,
            # 'unassignments[0][contextid]': self.api.get_contextid_for_courseid(course_id),
        }


        alreadythere = self._user_has_role_in_course(user_id, course_id, role_id)
        if not alreadythere:
            logger.debug(f"User already {user} not enrolled in role {role} in course {course_id}")
            return None
        data = self.api.execute(requests.post, params, dryrun_result=True)

        if data is None:
            logger.info(f"User {user_id} unenrolled from role {role_id} in course {course_id}")
            return {
                "user_id": user_id,
                "course_id": course_id,
                "role_id": role_id,
                "num_roles_deleted": 1
            }
        logger.error(f"Failed to unenrol user {user} from course {course_id}. API response: {data}")
        return None

    def _user_has_role_in_course(self, user_id: int, course_id: int, role_id: int) -> bool:
        """
        Check if a user still has a specific role in a course
        :param user_id: int: the user ID
        :param course_id: int: the course ID
        :param role_id: int: the role ID
        :return: bool: True if the user has the role, False otherwise
        """
        enrolled_users = self.get_enroled_users(course_id)
        result =  any(
            user['user_id'] == user_id and user['role_id'] == role_id
            for user in enrolled_users
        )
        logger.debug(f"User {user_id} has role {role_id} in course {course_id}: {result}")
        return result

    def course_delete_user(self, user: Union[int, str], course: Union[str, int]) \
            -> Union[None, Dict[str, int]]:
        """
        Delete a user from a course.

        :param user: int or str: the user id or username
        :param course: int or str: the course id or shortname
        :return: dict of user deleted with counts 1, or None if no change
        :raises ValueError: if the user or course does not exist
        """
        user_id = self.api.get_user_id(user)
        if user_id is None:
            logger.error(f"User does not exist: {user}")
            raise ValueError(f"User does not exist: {user}")

        course_id = self.api.get_course_id(course)
        if course_id is None:
            logger.error(f"Course does not exist: {course_id}")
            raise ValueError(f"Course does not exist: {course_id}")

        params = {
            'wsfunction': 'enrol_manual_unenrol_users',
            'enrolments[0][userid]': user_id,
            'enrolments[0][courseid]': course_id
        }

        data = self.api.execute(requests.post, params)

        if data is None:  # The API returns None on success
            # logging.info(f"User {user_id} deleted from  course {course_id}")
            # check to make sure this happened.
            if not any([enrol['user_id'] == user_id for enrol in self.get_enroled_users(course_id)]):
                logger.info(f"User {user_id} deleted from course {course_id}")
                return {"user_id": user_id, "course_id": course_id,
                        "num_roles_deleted": 1, "num_participations_deleted": 1}
            else:
                logger.debug(f"User {user_id} not in course {course_id} to delete.")
                return None
        else:
            logger.error(f"Failed to delete user {user} from course {course_id}. API response: {data}")
            raise Exception(f"Failed to delete user {user} from course {course_id}. API response: {data}")
        pass

    def __xxx_get_course_context_id(self, course_id: int) -> Union[int, None]:
        """
        Get the context ID for a given course.  removed because it requires a plugin.
        :param course_id: int: the course ID
        :return: int: the context ID for the course, or None if not found
        """
        params = {
            'wsfunction': 'core_course_get_courses',
            'options[ids][0]': course_id
        }

        try:
            data = self.api.execute(requests.get, params)
            if data and len(data) > 0:
                return data[0].get('contextid')
            else:
                config.logger.error(f"Failed to get context ID for course {course_id}")
                return None
        except requests.exceptions.RequestException as e:
            config.logger.error(f"API call failed when trying to get context ID for course {course_id}: {str(e)}")
            return None

class MoodleAPIUserProvider(MoodleUserProvider):



    def __init__(self, site, api_key):
        super().__init__()
        self.api = MoodleAPI(site, api_key)
        self.default_auth_method = 'manual'


    def create_user(self, username: str,email: str, firstname: str, lastname: str,
                    auth: str = None, password:str = None, **kwargs) -> Union[None, int]:

        if password is None:
            import random
            # generate a random 8 digit password of numbers
            password = str(random.randint(10000000, 99999999))
        if auth is None:
            auth = self.default_auth_method

        new_id = self.api.create_user(username, email, firstname, lastname, auth, password)
        return new_id


    def get_user(self, email_username_or_id: Union[str, int]) -> Union[None, dict]:
        """
        Get the user for the given email or user ID.
        :param email_username_or_id: str or int: the email address or user ID
        :return: dict: the user dict, or None if the user doesn't exist
        """
        user = self.api.get_user(email_username_or_id)
        return user

