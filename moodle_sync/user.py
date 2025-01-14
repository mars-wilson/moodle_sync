# file: moodle_sync/enrolment.py
from typing import List, Dict, Callable, Union, Set

from moodle_sync.config import config
from moodle_sync.logger import logger

class MoodleUserProvider:


    fields = [
        # these are the columns that will be used to synchronize user data.
        # 'id',  # for Moodle providers this will be the ID in the Moodle database.  Could be ID in your ERP tho.
        'username',
        'email',
        'firstname',
        'lastname',
        'auth',
        'password',
    ]

    def __init__(self):
        self.delete_unenroled_users = False
        pass

    def get_user(self, email_username_or_id: str) -> Union[None, Dict]:
        """
        Here is data with Moodle keys that this needs to return.
        Note that some fields, like "id", may not necessarily correspond to the Moodle database if pulled from
        a different source
        {
        'id': 3, 'username': 'wflintrock', 'firstname': 'Wilma', 'lastname': 'Flintrock', 'fullname': 'Wilma Flintrock',
        'email': 'wwwilmaflint@warren-wilson.edu', 'department': '', 'institution': '253NNN',
        'idnumber': '269A918F-C1D2-4466-B128-asdfasdfasdf', 'firstaccess': 1495797486, 'lastaccess': 1719747709,
        'auth': 'ldap', 'suspended': False, 'confirmed': True, 'lang': 'en_us', 'theme': '', 'timezone': '99',
        'mailformat': 1, 'description': '', 'descriptionformat': 1, 'city': 'Not Available', 'country': 'US',
        'profileimageurlsmall': 'https://example.warren-wilson.edu/pluginfile.php/NNNN/user/icon/boost/f2?rev=NNNN',
        'profileimageurl': 'https://example.warren-wilson.edu/pluginfile.php/NNNN/user/icon/boost/f1?rev=NNNN'
        }

        :return:
        """
        raise RuntimeError('Not Implemented. Derived Class needs get_user.')
        pass

    def get_all_users(self) -> List[Dict]:
        """
        Return a list of all users in the system.
        :return: List[Dict]
        """
        raise RuntimeError('Not Implemented. Derived Class needs get_all_users.')
        pass

    def create_user(self, username: str, email: str, firstname: str, lastname: str,
                    auth: str = None, password: str = None,
                    **kwargs) -> Union[int, None]:
        """
        Create a user with the given properties.  You can call with a dict if you splat it out.
        If you do not provide the auth method, the provder might override it to be the default in the instance.
        If you do not provide the password it should be ranzomized.
        :param username:
        :param auth:
        :param email:
        :param firstname:
        :param lastname:
        :param password:
        :param kwargs:
        :return:
        """
        raise RuntimeError('Not Implemented. Derived Class needs create_user.')
        pass

    def get_user_id(self, email_username_or_id: str) -> Union[None, int, Dict]:
        """
        Return the user for a username.
        :param email_username_or_id: str: The username or email address.  Usually username should be used.
        :return: None or the dict for the user.
        """
        raise RuntimeError('Not Implemented. Derived Class needs get_user_id.')
        pass

    def get_username(self, user_id: int) -> Union[None, str]:
        """
        Return the username for a user id.
        :param user_id: int: The user id.
        :return: str: The username.
        """
        raise RuntimeError('Not Implemented. Derived Class needs get_username.')
        pass



class UserSync:
    def __init__(self, target: MoodleUserProvider, source: MoodleUserProvider):
        self.target = target
        self.source = source

    def sync(self):
        """

        Suggestion for sync:
            pull a list of classes, and if the class is in a cancelled state
            actually delete all the users from the class.  But if it is not in the Canceled state, then add
            or remove the relevant role.
        Sync enrollments from the source provider to Moodle.
        """

        source_users = self.source.get_all_users()
        cnt_created, cnt_exists = 0, 0
        logger_line = ""
        for user in source_users:
            username = user['username']
            if self.target.get_user(username) is None:
                if logger_line: logger.info(logger_line)
                logger_line = ''
                logger.info(f"User {username} not found in target.  Creating.")
                self.target.create_user(**user)
                cnt_created += 1
            else:
                logger_line += (' ' if logger_line else "Exists: ") + username
                cnt_exists += 1
            if len(logger_line) > 80:
                logger.info(logger_line)
                logger_line = ""
        if logger_line: logger.info(logger_line)
        logger.info(f"Enrollment sync complete. Users Created: {cnt_created}  Existing: {cnt_exists} total {cnt_created + cnt_exists}")
