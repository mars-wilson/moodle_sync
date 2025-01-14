# file: tests/provider_mysql.py

# import pytest

from tests.test_setings import TEST_MOODLE_SITE, TEST_MOODLE_API_KEY, settings
from moodle_sync.provider_moodleapi import MoodleAPICourseProvider
import moodle_sync.util as util
from moodle_sync.config import config

"""
This test basically runs a bunch of stuff.  
It creates a course, updates it, and then checks that the updates were made.
You'll have to manually delete the course after running it.
If you run it twice in a row, it'll throw a course exists exception, which it should!

Note that you can run "test_settings.py" in the python console to define these things.
"""

test_category = settings.get('moodle_course_category_name', 'Example Courses')
test_category_parent = settings.get('moodle_course_category_parent')

print("Example Test Category:", test_category, "Parent:", test_category_parent)
cred = TEST_MOODLE_API_KEY
m = MoodleAPICourseProvider(TEST_MOODLE_SITE, cred)
self = m  # for running in python console

print("Site is: ", TEST_MOODLE_SITE)

# for run in console:
import requests
import json
import logging
import keyring



config.debug = False

if __name__ == '__main__':
    # fix the categories.
    try:
        print("Getting parent category ", test_category_parent)
        parentcategory = m.get_category(test_category_parent)
    except:
        print("Not Found?\nCreating parent category ", test_category_parent)
        parentcategory = m.create_category(test_category_parent)
        print("Parent Category ID:", parentcategory)
        print("Created the parent category.")
        parentcategory = m.get_category(test_category_parent)
        print("Parent Category ID:", parentcategory)

    try:
        categoryid = m.get_category(test_category)
        print("Category ID:", categoryid)
    except ValueError as e:
        print("Exception:", e)
        print("Creating the category.")
        categoryid = m.create_category(test_category, test_category_parent)
        print("Category ID:", categoryid)

    courses = m.get_courses(field='category', value=categoryid)
    print("Courses in category:", len(courses))

    shortname = settings.get('moodle_course_shortname', 'test_course_api')

    #existing_course_id = m._get_course_id_by_shortname(shortname)
    existing_course = m.get_course(shortname)
    print("Existing Course ID: ", existing_course['id'] if existing_course else 'NONE - not created yet.')
    course = {
        'fullname': 'Test Course API',   # Required: The full name of the course
        'shortname': shortname,          # Required: The short name of the course
        'categoryid': categoryid,        # Required: The category id the course belongs to
        'summary': '<h1>This is a summary</h1><p>Dude.</p>',  # Optional: The summary of the course
        'format': 'weeks',               # Optional: Course format such as 'weeks', 'topics'
        'numsections': 10,               # Optional: Number of sections in the course
        'startdate': util.unix_timestamp('2024-04-15'),         # Optional: Start date of the course (Unix timestamp)
        'enddate': util.unix_timestamp('2025-04-15'),           # Optional: End date of the course (Unix timestamp)
        # ... other optional fields
    }
    try:
        print("Creating course ", shortname, "categoryid", categoryid)
        m.create_course(course)
    except Exception as e:
        print("Exception:", e)


    courses = m.get_courses(field='shortname', value=shortname)
    if courses:
        course = courses[0]
        print("Course: \n   " + "\n   ".join([f"{k:15}: {v}" for k, v in course.items()]))
    else:
        print("No matching course found.  Debug is ", config.debug)

    course['format']  = 'topics'  # should not be updated
    course['enddate'] =  util.unix_timestamp('2025-04-16')
    print("Updating course")
    m.update_course(course)
    courses_updated = m.get_courses(field='shortname', value=shortname)
    if courses_updated:
        course_updated = courses_updated[0]
    else:
        print("No matching course found.  Debug is ", config.debug)
    assert course_updated['format'] == 'weeks', "format should not have been updated"
    assert course_updated['enddate'] == util.unix_timestamp('2025-04-16'), "enddate should have been updated"

