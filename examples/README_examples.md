<!-- file:  examples/README.md -->

# Project moodle_sync Examples

## jenzabar_to_moodle.py:

Example script for synchronizing courses and enrolments 
from Jenzabar J1 ERP to Moodle.  Jenzabar uses MSSQL and Moodle uses the API.

API credentials are stored using the keychain module.

Jenzabar MSSQL connections utilize user authentication.

Includes two sample views:
- jenzabar_courses - A view of courses in Jenzabar
- jenzabar_enrolments - A view of enrolments in Jenzabar

