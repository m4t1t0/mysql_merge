import MySQLdb
import MySQLdb.cursors
import _mysql_exceptions
import sys
import traceback
from mysql_merge.levenshtein import levenshtein_lowest
from mysql_merge.config import verbose

qs = ""

class MiniLogger(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(MiniLogger, cls).__new__(
                cls, *args, **kwargs)
        return cls._instance

    def __set__(self, k, v):
        if k == "qs":
            qs = v
        setattr(self, k, v)

    def log(self, v):
        if verbose:
            print v


def lists_diff(a, b):
    b = set(b)
    return [aa for aa in a if aa not in b]


def create_connection(connection, common_data={}):
    try:
        data = dict(common_data)
        data.update(connection)

        return MySQLdb.connect(data['host'], data['user'], data['password'], data['db'],
                               cursorclass=MySQLdb.cursors.DictCursor)

    except Exception, e:
        handle_exception("Exception on connecting to the database", e, )


logger = MiniLogger()


def handle_exception(custom_message, exception, connection=None):
    print ""
    print "-----------------------------------------------"
    print custom_message
    if logger.qs:
        print "Last query was: "
        print logger.qs
    print "The error message is: "
    print exception
    print "The traceback is: "
    traceback.print_tb(sys.exc_info()[2])
    #print ""
    #if connection:
    #    print "Rollback"
    #    connection.rollback()
    #print ""
    sys.exit()
