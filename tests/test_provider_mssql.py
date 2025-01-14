# file: tests/test_provider_mssql.py

from tests.test_setings import settings, MSSQL_CONNECTION_STRING

from moodle_sync.provider_mssql import MoodleMSSQLProvider

if not MSSQL_CONNECTION_STRING:
    raise ValueError(f"No connection string found for mssql.  Define one in test_settings or put it in the keyring.")


m = MoodleMSSQLProvider(MSSQL_CONNECTION_STRING, '_wwn_moodle_courses')

courses = m.get_courses()

