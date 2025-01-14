# file:  test_settings.py

import os
import keyring
import json


__all__ = ['TEST_MOODLE_SITE', 'TEST_MOODLE_API_KEY', 'settings',
           'MSSQL_CONNECTION_STRING']

try:
    settings = json.loads(open('test_settings.json').read())
except FileNotFoundError:
    print("""
    No test settings.  Try putting a file named test_settings.json in the root of the project.
    
    Example test_settings.json:
    
    {
        "moodle_site": "moodle.myschool.edu",
        "moodle_api_servicename": "moodle.myschool.edu",
        "moodle_api_username": "moodle_api"
    }
    
    """)
    settings = {}


#
#          Moodle things
#

TEST_MOODLE_SITE = settings.get('moodle_site', 'moodle.myschool.edu')

moodle_api_servicename = settings.get('moodle_api_servicename', TEST_MOODLE_SITE)
moodle_api_username    = settings.get('moodle_api_username', 'moodle_api')
try:
    TEST_MOODLE_API_KEY = keyring.get_password(moodle_api_servicename, moodle_api_username)
except keyring.errors.KeyringError:
    print(f"No keyring entry found for service {moodle_api_servicename} user {moodle_api_username}.")
    TEST_MOODLE_API_KEY = input(f"Enter the API key for {moodle_api_username}@{moodle_api_servicename}: ")
    keyring.set_password(moodle_api_servicename, moodle_api_username, TEST_MOODLE_API_KEY)

#
# ################### MSSQL THINGS
#

mssql_connection_keyring_service = settings.get('mssql_connection_keyring_service', 'moodle_mssql_connection')

MSSQL_CONNECTION_STRING = settings.get('moodle_mssql_connection',
                                       keyring.get_password(mssql_connection_keyring_service,
                                                            'connection_string'))




