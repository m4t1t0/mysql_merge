import sys
from mysql_merge.utils import MiniLogger, create_connection, handle_exception
from mysql_merge.mysql_mapper import Mapper
from mysql_merge.utils import lists_diff
import MySQLdb
import warnings


class Merger(object):
    _conn = None
    _cursor = None

    _source_mapper = None
    _db_map = None
    _destination_db_map = None

    _config = None
    _logger = None
    _counter = 0

    _destination_db = None
    _source_db = None

    _increment_step = 0
    _increment_value = property(lambda self: self._counter * self._increment_step)

    def __init__(self, destination_db_map, source_db, destination_db, config, counter, logger):
        self._destination_db_map = destination_db_map

        self._increment_step = config.increment_step
        self._source_db = source_db
        self._destination_db = destination_db
        self._config = config
        self._counter = counter
        self._logger = logger

        self._conn = create_connection(self._source_db)
        self._cursor = self._conn.cursor()

        self.prepare_db()

        self._logger.log("Processing database '%s'..." % self._source_db['db'])
        # Indexes may be named differently in each database, therefore we need
        # to remap them

        self._logger.log(" -> Re-mapping database")
        self._source_mapper = Mapper(self._conn, source_db['db'], config, MiniLogger(), verbose=False)
        db_map = self._source_mapper.map_db()
        self._db_map = db_map

    def prepare_db(self):
        cur = self._cursor

        self._logger.qs = "set names utf8"
        cur.execute(self._logger.qs)

        warnings.filterwarnings('error', category=MySQLdb.Warning)

    def __del__(self):
        if self._cursor:
            self._cursor.close()

        if self._conn:
            self._conn.close()

    def merge(self):
        self._conn.begin()

        self._logger.log(" ")
        self._logger.log("Processing database '%s'..." % self._source_db['db'])

        self._logger.log(" -> 1/6 Executing preprocess_queries (specified in config)")
        self.execute_preprocess_queries()

        self._logger.log(" -> 2/6 Re-mapping database")
        self._source_mapper = Mapper(self._conn, self._source_db['db'], self._config, MiniLogger(), verbose=False)
        db_map = self._source_mapper.map_db()
        self._db_map = db_map

        self._logger.log(" -> 3/6 Incrementing PKs")
        #Do not touch the main DB
        if (self._source_db['db'] != self._config.main_db):
            self.increment_pks()

        self._logger.log(" -> 4/6 Incrementing FKs")
        #Do not touch the main DB
        if (self._source_db['db'] != self._config.main_db):
            self.increment_fks()

        self._logger.log(" -> 5/6 Copying data to the destination db")
        self.copy_data_to_target()

        self._logger.log(" -> 6/6 Committing changes")
        self._conn.commit()
        self._logger.log("----------------------------------------")

    def execute_preprocess_queries(self):
        cur = self._cursor

        for q in self._config.preprocess_queries:
            try:
                self._logger.qs = q
                cur.execute(self._logger.qs)
            except Exception, e:
                handle_exception(
                    "There was an error while executing preprocess_queries\nPlease fix your config and try again",
                    e, self._conn)

    def increment_pks(self):
        cur = self._cursor

        for table_name, table_map in self._db_map.items():
            if (table_name in self._config.exclude_tables):
                continue

            increment_value = self.get_increment_value_table(table_name)

            for col_name, col_data in table_map['primary'].items():
                try:
                    self._logger.qs = "UPDATE `%(table)s` SET `%(pk)s` = `%(pk)s` + %(step)d" % {"table": table_name,
                                                                                                 "pk": col_name,
                                                                                                 'step': increment_value}
                    cur.execute(self._logger.qs)
                except Exception, e:
                    handle_exception("There was an error while updating PK `%s`.`%s` to %d + pk_value" % (
                    table_name, col_name, self._increment_value), e, self._conn)

    def get_increment_value_table(self, table_name):
        for table_data in self._config.custom_increment_step:
            if (table_data['table'] == table_name):
                return table_data['value']

        return self._increment_value

    def increment_fks(self):
        cur = self._cursor

        set_clause = ""
        joiner = ""
        for table_data in self._config.fk_mapping:
            set_clause = ""
            joiner = ""

            if (table_data['fields'] == None):
                continue

            for field in table_data['fields']:
                if (set_clause != ""):
                    joiner = ", "

                set_clause += joiner + "`%(field)s` = `%(field)s` + %(step)d" % {"field": field,
                                                          'step': self._increment_value} 

            try:
                self._logger.qs = "UPDATE `%(table)s` SET %(set_clause)s" % {"table": table_data['table'],
                                                                             "set_clause": set_clause}
                cur.execute(self._logger.qs)
            except Exception, e:
                handle_exception("There was an error while updating FKs in table `%s`" % (
                table_data['table']), e, self._conn)

    def copy_data_to_target(self):
        cur = self._cursor

        diff_tables = self._source_mapper.get_non_overlapping_tables(self._destination_db_map)
        for k, v in diff_tables.items():
            if len(v):
                self._logger.log("----> Skipping some missing tables in %s database: %s; " % (k, v))

        # Copy all the data to destination table
        for table_name, table_map in self._db_map.items():
            if any([table_name in v for k,v in diff_tables.items()]):
                continue

            if (self._source_db['db'] != self._config.main_db):
                if (table_name in self._config.exclude_tables):
                    continue

            try:
                diff_columns = self._source_mapper.get_non_overlapping_columns(self._destination_db_map, table_name)
                for k, v in diff_columns.items():
                    if len(v):
                        self._logger.log(
                            "----> Skipping some missing columns in %s database in table %s: %s; " % (k, table_name, v))

                columns = self._source_mapper.get_overlapping_columns(self._destination_db_map, table_name)
                if not len(columns):
                    raise Exception("Table %s have no intersecting column in merged database and destination database")

                self._logger.qs = "INSERT IGNORE INTO `%(destination_db)s`.`%(table)s` (%(columns)s) SELECT %(columns)s FROM `%(source_db)s`.`%(table)s`" % {
                    'destination_db': self._destination_db['db'],
                    'source_db': self._source_db['db'],
                    'table': table_name,
                    'columns': "`%s`" % ("`,`".join(columns))
                }
                cur.execute(self._logger.qs)
            except Exception, e:
                hint = "--> HINT: Looks like you runned this script twice on the same database\n" if "Duplicate" in "%s" % e else ""
                handle_exception(
                    ("There was an error while moving data between databases. Table: `%s`.\n" + hint) % (table_name), e,
                    self._conn)

  