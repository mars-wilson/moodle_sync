
"""
This is a base import that defines global configuration variables that you can then override if you wish.
"""
__all__ = ['config']


class Config:
    # A singleton config class
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, debug=False, dryrun=False):
        if self._initialized:
            print("Returning existing instance.")
            return
        self._debug = debug
        self.dryrun = dryrun

    @property
    def debug(self):
        return self._debug         # are we in debuggy the mode?  If so log detailed messages.

    @debug.setter
    def debug(self, value: bool):
        # allow to set and unset logger level.
        from moodle_sync.logger import logger
        import logging
        logger.setLevel(logging.DEBUG & logging.INFO & logging.ERROR if value else logging.INFO & logging.ERROR)
        self._debug = value


config = Config()

