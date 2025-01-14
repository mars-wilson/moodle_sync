import logging
from typing import List, Any

"""
Provide an extensible logging method that protects wptoken and other passwords.

The safe_print method will print the args and kwargs.   It scans thses  for dicts,
and replaces wptoken or 'password' keys with ***.
You may need to set safe_print to be your own method if you are logging content not in a dict.

The module only uses info, debug, and error loggers.
Set the debug flag in config.

There is also a dryrun flagd in config that you can set to True to prevent changes from being made.

"""

__all__ = ['debug', 'info', 'warning', 'error', 'critical', 'logger']

secure_dict_keys = ['wstoken', 'password']

def mask_for_log_dict(thingy: dict) -> dict:
    # take the secure stuff out of a thingy dict.
    return {k: '***' if k in secure_dict_keys else v for k, v in thingy.items()}


def safe_print(*args, **kwargs):
    """
    Print the args and kwargs.  If there is an exception, print the exception.
    :param args:
    :param kwargs:
    :return:
    """
    # detokenize any dicts in the args, then pass everything to print.
    detokenized_args = [mask_for_log(arg) if isinstance(arg, dict) else arg for arg in args]
    kwargs = mask_for_log(kwargs)
    print(*detokenized_args, **kwargs)


def mask_for_log(obj: Any) -> Any:
    """
    Mask sensitive information in dictionaries.
    For other types, return the object as is.
    """
    if isinstance(obj, dict):
        return {k: '***' if k in ['wstoken', 'password'] else mask_for_log(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [mask_for_log(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(mask_for_log(item) for item in obj)
    return obj

def safe_format(msg: str, *args: Any, **kwargs: Any) -> str:
    masked_args = [mask_for_log(arg) for arg in args]
    masked_kwargs = {k: mask_for_log(v) for k, v in kwargs.items()}

    try:
        if kwargs:
            return msg.format(*masked_args, **masked_kwargs)
        elif args:
            return msg.format(*masked_args)
        else:
            return msg
    except:
        # Fallback to concatenation if formatting fails
        return f"{msg} {' '.join(map(str, masked_args))} {' '.join(f'{k}={v}' for k, v in masked_kwargs.items())}".strip()


class SafePrintHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            if record.args:
                # Handle tuple arguments
                if len(record.args) == 1 and isinstance(record.args[0], (dict, tuple, list)):
                    msg = f"{msg}: {mask_for_log(record.args[0])}"
                else:
                    # Use safe_format which includes masking
                    msg = safe_format(msg, *record.args)
            elif isinstance(record.msg, dict):
                # If the message itself is a dictionary, mask it
                msg = f"{msg}: {mask_for_log(record.msg)}"
            print(msg)
        except Exception as e:
            print(f"Error in SafePrintHandler: {e}")
            print(f"Original message: {mask_for_log(record.msg)}")
            if record.args:
                print(f"Arguments: {mask_for_log(record.args)}")



#
#  None of the above stuff works right.  So FLOSKDHJF:LS.
#




class MyLogger:
    def __init__(self, name: str):
        self.name = name
        self.level = logging.INFO & logging.ERROR & logging.DEBUG
        #print(self.level, logging.INFO, logging.ERROR, logging.DEBUG)
    def setLevel(self, level):
        self.level = level

    def debug(self, *args, **kwargs):
        #print("dbg", self.level, logging.INFO, logging.ERROR, logging.DEBUG)
        if True or self.level & logging.DEBUG != 0:
            safe_print(*args, **kwargs)

    def info(self, *args, **kwargs):
        #print(self.level, logging.INFO, logging.ERROR, logging.DEBUG)
        #print("INFO!")
        if True or self.level & logging.INFO != 0:
            safe_print(*args, **kwargs)

    def error(self, *args: tuple, **kwargs):

        if True or self.level & logging.ERROR != 0:
            args_w_error = ('Error: ',) + args
            safe_print(*args_w_error, **kwargs)




    @classmethod
    def setFormatter(cls, formatter):
        pass

    @classmethod
    def addHandler(cls, handler):
        pass


logging_info = logging.INFO  # store the default
logging_error = logging.ERROR
logging_debug = logging.DEBUG

class logging:
    mylogger = None
    INFO = logging_info
    ERROR = logging_error
    DEBUG = logging_debug


    @classmethod
    def getLogger(cls, name):
        if not cls.mylogger:
            cls.mylogger = MyLogger(name)
        return cls.mylogger


    @classmethod
    def Formatter(cls, fmt):
        return fmt

def setup_logging(level=logging.INFO):
    logger = logging.getLogger('moodle_sync')
    logger.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    handler = SafePrintHandler()
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


# Create logger instance
logger = setup_logging()

# Create convenience methods
debug = logger.debug
info = logger.info
#warning = logger.warning
error = logger.error
#critical = logger.critical

if __name__ == "__main__":
    logger.info("This is a test")
    # or
    logger.info("This is a test with a tuple", (1,2,3))
    logger.info("this is a test with a dict", {'wstoken': "123", 5: 18})

