# file: moodle_sync/enrolment.py
from typing import List, Dict, Callable, Union, Set

from moodle_sync.config import config
from moodle_sync.logger import logger

class MoodleEnrolmentProvider:

    fields = [
        # these are the columns that will be used to synchronize course enrolments.
        # All providers must return these dict keys with the proper types!  up to providers to cast to the right type.
        'shortname',
        'role',
        'username',
        # Non-Moodle provider fields.
        'course_status', # a custom field not in the Moodle course table.  Used to determine if the course cancelled
        'started',  # 0 or 1.  If the course hasn't started (or the user hasn't started the course) this can be 0
                # students not started but not enrolled can usually be deleted.
    ]

    roles = [
        {   "id": 0, "name": "None", "shortname": "none", "sortorder": 0, "archetype": "",
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



    def __init__(self):
        self.delete_unenroled_users = False
        pass

    def cancelled(self, course) -> bool:
        # You can override this function to determine cancelled state in a different way.
        return False

    # moved to user.py
    # def get_user(self, email_username_or_id: str) -> Union[None, Dict]:
    #     """
    #     Here is data with Moodle keys that this needs to return.
    #     Note that some fields, like "id", may not necessarily correspond to the Moodle database if pulled from
    #     a different source
    #     {
    #     'id': 3, 'username': 'wflintrock', 'firstname': 'Wilma', 'lastname': 'Flintrock', 'fullname': 'Wilma Flintrock',
    #     'email': 'wwwilmaflint@warren-wilson.edu', 'department': '', 'institution': '253NNN',
    #     'idnumber': '269A918F-C1D2-4466-B128-asdfasdfasdf', 'firstaccess': 1495797486, 'lastaccess': 1719747709,
    #     'auth': 'ldap', 'suspended': False, 'confirmed': True, 'lang': 'en_us', 'theme': '', 'timezone': '99',
    #     'mailformat': 1, 'description': '', 'descriptionformat': 1, 'city': 'Not Available', 'country': 'US',
    #     'profileimageurlsmall': 'https://example.warren-wilson.edu/pluginfile.php/NNNN/user/icon/boost/f2?rev=NNNN',
    #     'profileimageurl': 'https://example.warren-wilson.edu/pluginfile.php/NNNN/user/icon/boost/f1?rev=NNNN'
    #     }
    #
    #     :return:
    #     """
    #     raise RuntimeError('Not Implemented. Derived Class needs get_user.')
    #     pass


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

    def get_role_id(self, role: str) -> Union[None, int]:
        """
        Return the role id for a role name.
        :param role: str: The role name.
        :return: int: The role id.
        """
        raise RuntimeError('Not Implemented. Derived class needs get_role_id.')
        pass

    def rolename_for_id(self, role_id: int) -> Union[None, str]:
        """
        Return the role name for a role id.
        :param role_id: int: The role id.
        :return: str: The role name.
        """
        return next((role['shortname'] for role in self.roles if role['id'] == role_id), None)
        pass

    def get_course_id(self, shortname: str) -> Union[None, int]:
        """
        Return the course id for a shortname.  This is probably from Moodle only.
        :param shortname: str: The shortname.
        :return: int: The course id.
        """
        raise RuntimeError('Not Implemented. Derived class needs get_course_id.')
        pass

    def get_enroled_users(self, course_id: int) -> Union[None, List[Dict]]:
        """
        Return a list of enrollments for a course.
        :param course_id: int: The Moodle course id.
        :return: list: A list of enrollments.
        """
        raise RuntimeError('Not Implemented. Derived class needs get_enrolments.')
        pass

    def get_course_shortnames_for_sync(self, course:str = None) -> Set:
        """
        Return a list of all course shortnames that need enrolment sync.
        :param course:
        :return: set
        """
        raise RuntimeError('Not Implemented. Derived class needs get_enrolments.')
        pass


    def course_delete_user(self, user_id: int, course_id: int) -> Union[None, Dict]:
        """
        Delete a user from a course.
        :param user_id: int: The user id.
        :param course_id: int: The course id.
        :return dict('user_id': int, 'course_id': int,  'num_roles_deleted': int, 'num_participations_deleted': int):
            The user id, course id, and number of roles and participations deleted.  May be >0 for success.
            Returns None if nothing done.
        """
        pass

    def course_enrol_user(self, user_id: int, course_id: int, role_id: int) -> Union[None, Dict]:
        """
        Enrol a user from a course in the given role ID
        :param user_id: int: The user id.
        :param role_id: int: The role id.
        :param course_id: int: The course id.
        :return dict('user_id': int, 'course_id': int, 'role_id': int,
                 "num_new_enrols": int, "num_roles_added": int}): The user id, course id, and role id.
                 Numbers may be >0 to indicate success.
                 Returns None if nothing done.
        """
        pass

    def course_unenrol_user(self, user_id: int, course_id: int, role_id: int) -> Union[None, Dict]:
        """
        Unenrol a user from a course in the given role ID
        :param user_id: int: The user id.
        :param role_id: int: The role id.
        :param course_id: int: The course id.
        :return dict('user_id': int, 'course_id': int, 'role_id': int,
                    "num_roles_deleted": int) : The user id, course id, and role id.
                    Numbers may be >0 to indicate success.
                    Returns None if nothing done.
        """
        pass


class EnrolmentSync:
    def __init__(self, target: MoodleEnrolmentProvider, source: MoodleEnrolmentProvider):
        self.target = target
        self.source = source
        self.roles_to_add = ['student', 'editingteacher']
        self.roles_to_remove = ['student']  # don't by default remove teachers - they may be manually added.

    def sync_users(self):
        """
        Users could be synced before enrollments!  That way, everyone will exist before the course

        :return:
        """

    def sync_to_moodle(self):
        """

        Suggestion for sync:
            pull a list of classes, and if the class is in a cancelled state
            actually delete all the users from the class.  But if it is not in the Canceled state, then add
            or remove the relevant role.
        Sync enrollments from the source provider to Moodle.

        The source enrollment provider provides the shortname (used as the course_id by default), username, and role.
        Those have to be mapped to the moodle course_id, user_id, and role_id.

        """
        source_courses = self.source.get_course_shortnames_for_sync()
        logger.info(f"Found {len(source_courses)} courses to sync enrollments.")

        cnt_added, cnt_deleted, cnt_updated, cnt_error, cnt_unenrolled = 0, 0, 0, 0, 0
        logger.info(f"Syncing enrollments for {len(source_courses)} courses.")
        for source_shortname in source_courses:
            if True: # try:
                course_id = self.target.get_course_id(source_shortname)
                logger.info(f"Syncing enrollments for course: {source_shortname} id {course_id}")

                if course_id is None:
                    logger.error(f"  Course not found in Moodle: {source_shortname}")
                    continue

                source_enrollments = self.source.get_enroled_users(source_shortname)
                moodle_enrollments = self.target.get_enroled_users(course_id)
                logger.info(f"  Found {len(source_enrollments)} source enrollments"
                            f" and {len(moodle_enrollments)} Moodle enrollments for course: {source_shortname}")
                if self.source.cancelled(source_shortname):
                    # Remove all users from the cancelled course
                    logger.info(f"  Removing all users from cancelled course: {source_shortname}")
                    for enrollment in moodle_enrollments:
                        logger.info(f"***  Removing user {enrollment['username']} from cancelled course {source_shortname}")
                        self.target.course_delete_user(enrollment['user_id'], course_id)
                    logger.info(f"Removed all users from cancelled course: {source_shortname}")
                else:
                    # push source to moodle:
                    for source_enrollment in source_enrollments:
                        user_id: int = self.target.get_user_id(source_enrollment['username'])
                        role_id: int = self.target.get_role_id(source_enrollment['role'])
                        if user_id is None:
                            logger.info(f"*** User not found: {source_enrollment['username']}")
                            continue
                        if role_id is None:
                            logger.info(f"*** role not found: {source_enrollment['role']}")
                            continue

                        # moodle_enrollment = next((e for e in moodle_enrollments if e['user_id'] == user_id), None)
                        moodle_user_roles = [e['role_id'] for e in moodle_enrollments if e['user_id'] == user_id]
                        if role_id not in moodle_user_roles:
                            # Add new enrollment
                            logger.info(f"-- Adding user {source_enrollment['username']} role {role_id} to course {source_shortname}. Existing roles: {moodle_user_roles}")
                            self.target.course_enrol_user(user_id, course_id, role_id)
                            if not moodle_user_roles:
                                cnt_added += 1
                            else:
                                cnt_updated += 1
                        else:
                            # logger.debug(f"      ___ user  {source_enrollment['username']} already in role {role_id} to course {source_shortname}")
                            pass
                        # elif moodle_enrollment['role_id'] != role_id:
                        #     # Update role
                        #     # logger.info(f"Updating role for user {source_enrollment['username']} in course {source_shortname}")
                        #     # if self.moodle.rolename_for_id(moodle_enrollment['role_id']) in self.roles_to_remove:
                        #     #     logger.info(f"Removing user {source_enrollment['username']} from course {source_shortname}")
                        #     #     self.moodle.course_unenrol_user(user_id, course_id, moodle_enrollment['role_id'])
                        #     #
                        #     # self.moodle.course_unenrol_user(user_id, course_id,  moodle_enrollment['role_id'])
                        #     logger.info(
                        #         f"-- Adding user {source_enrollment['username']} role {role_id} to course {source_shortname} -.")
                        #
                        #     self.moodle.course_enrol_user(user_id, course_id, role_id)
                        #     cnt_updated += 1

                    # Remove enrollments that are in Moodle but not in the source unless they're not in roles_to_remove
                    for moodle_enrollment in moodle_enrollments:
                        user_id = moodle_enrollment['user_id']
                        course_id = moodle_enrollment['course_id']
                        role_id = moodle_enrollment['role_id']
                        if role_id > 0 and self.target.rolename_for_id(role_id) not in self.roles_to_remove:
                            continue
                        username = self.target.get_username(user_id)
                        user_in_source_course = any(e['username'] == username for e in source_enrollments)
                        if not user_in_source_course:
                            # the user was never in the course at all.  Remove them.
                            if self.target.delete_unenroled_users or not source_enrollment['started']:
                                logger.info(f"-- Deleting user {username} from course {source_shortname} - not in source."
                                            f"started? {'Yes' if source_enrollment['started'] else 'No'}")
                                self.target.course_unenrol_user(user_id, course_id, role_id)
                                self.target.course_delete_user(user_id, course_id)
                                cnt_deleted += 1
                            else:
                                logger.info(f"-- Unenrolling user {username} from course {source_shortname} - not in source.")
                                self.target.course_unenrol_user(moodle_enrollment['user_id'], course_id, moodle_enrollment['role_id'])
                                cnt_unenrolled += 1
                    # for step through enrollments for course


            if False: #except Exception as e:
                logger.info(f"Error syncing enrollments for course {course_shortname}: {str(e)}")
                cnt_error += 1

        logger.info(
            f"Enrollment sync complete. Added: {cnt_added}, Unenrolled: {cnt_unenrolled}, Deleted: {cnt_deleted}"
            f" Updated: {cnt_updated}, Errors: {cnt_error}")




def main():
    pass

if __name__ == "__main__":
    pass