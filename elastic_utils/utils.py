import time


def singleton(cls):
    instances = {}
    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]
    return getinstance


class Timer(object):
    def __init__(self, name='elapsed time', logger=None, print_result=False):
        self.verbose = print_result
        self.logger = logger
        self.name = name

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        if self.verbose:
            print(self.get_formatted_string())
        if self.logger:
            self.logger(self.get_formatted_string())

    def get_formatted_string(self):
        return '{}: {:.1f} ms'.format(self.name, self.msecs)


def new_index_from_name(base_name):
    """
    Return a new index name with a timestamp added at the end
    :param base_name: str
    :return: str
    """
    return base_name + "." + str(int(time.time()))
