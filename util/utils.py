import time
from datetime import datetime as dt
from functools import wraps
import configparser
import os
#from config import CLUSTERS_CFG_PATH, ROOT_DIR, CLUSTERS_CFG_FILENAME
from configure import config_instance as c
import errno


def between(value, start_a, end_b):
    """
    Find string in value between start_a and end_b exclusive

    :param value: The string in which find
    :param start_a: The starting character
    :param end_b:  The ending character
    :return:  the string between start_a and end_b
    """
    # Find and validate before-part.
    pos_a = value.find(start_a)
    if pos_a == -1: return ""
    # Find and validate after part.
    pos_b = value.find(end_b)
    if pos_b == -1: return ""
    # Return middle part.
    adjusted_pos_a = pos_a + len(start_a)
    if adjusted_pos_a >= pos_b: return ""
    return value[adjusted_pos_a:pos_b]


def timing(f):
    """
    Wrap a function to get the function's duration

    :param f: function to wrap
    :return:
    """

    @wraps(f)
    def wrap(*args):
        t_start = time.time()
        ret = f(*args)
        t_end = time.time()
        print('\n%s function took %0.3f ms' % (f.__name__, (t_end - t_start) * 1000.0))
        return ret

    return wrap


def string_to_datetime(time_string):
    """
    Fast convert from string to datetime

    :param time_string: the string of the time to convert
    :return: the converted datetime
    """
    split = time_string.split(":")
    if "." in time_string:
        split_2 = split[2].split(".")
        return dt(2016, 1, 1, int(split[0]), int(split[1]), int(split_2[0]), int(split_2[1]))
    else:
        return dt(2016, 1, 1, int(split[0]), int(split[1]), int(split[2]), 0)


def get_cfg():
    cfg = configparser.ConfigParser()
    cfg.read(c.CLUSTERS_CFG_PATH)
    return cfg


def write_cfg(cfg, output_path=c.ROOT_DIR):
    with open(os.path.join(output_path, c.CLUSTERS_CFG_FILENAME), 'w') as cfg_file:
        cfg.write(cfg_file)


class open_cfg:
    """
    context manager to open and automatically save config files
    """
    def __init__(self, r_path=c.CLUSTERS_CFG_PATH, mode='r', w_path=None):
        self.cfg = configparser.ConfigParser()
        self.r_path = r_path
        self.w_path = w_path if w_path else r_path
        self.cfg.read(self.r_path)
        self.mode = mode

    def __enter__(self):
        return self.cfg

    def __exit__(self, type, value, traceback):
        if self.mode == 'w':
            with open(os.path.join(self.w_path), 'w') as cfg_file:
                self.cfg.write(cfg_file)


def make_sure_path_exists(path):
    """"Check if the provided path exists. If it does not exist, create it."""
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def string_to_bool(string):
    """
    Convert from string to bool()

    :param time_string: the string to be converted
    :return: the converted bool() value (True|False)
    """
    return string in ['true', 'True','t', 'T', 'y', 'yes', 'Y', 'Yes' '1', 'one', 'One']