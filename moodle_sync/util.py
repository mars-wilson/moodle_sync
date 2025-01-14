# file: moodle_sync/util.py

from datetime import datetime, timezone
import time


def unix_timestamp(datestr: str, format='%Y-%m-%d',tzinfo=None) -> int:
    """
    Convert a SQL date string to a Unix timestamp.
    :param datestr: str: A date string - UTC date assumed.
    :param format:  Defaults to the format 'YYYY-MM-DD'.
    :param tzinfo:  Defaults to None. If None, UTC is assumed.  Otherwise a timezone object.
    :return: int: A Unix timestamp.
    """

    date_obj = datetime.strptime(datestr, format)
    if tzinfo is None:
        tzinfo = timezone.utc
    date_obj = date_obj.replace(tzinfo=tzinfo)
    unix_timestamp = int(time.mktime(date_obj.timetuple()))
    return unix_timestamp




if __name__ == '__main__':
    assert unix_timestamp('2023-04-15') == 1681531200

