import sys
import copy
import traceback
from collections import defaultdict
from mysql_merge.utils import MiniLogger, create_connection, handle_exception
from mysql_merge.mysql_mapper import Mapper
from mysql_merge.mysql_merger import Merger
import mysql_merge.config as config

# VALIDATE CONFIG:
if len(config.merged_dbs) == 0:
    print "You must specify at least one database to merge"
    sys.exit()
  
# Prepare logger

#####################################################################

# STEP 1 - map database schema, relations and indexes
print "STEP 1. Initial mapping of DB schema"
print " -> 1.1 First merged db"

mapped_db = config.merged_dbs[0]
conn = create_connection(mapped_db, config.common_data)

mapper = Mapper(conn, mapped_db['db'], config, MiniLogger())
db_map = mapper.map_db()

conn.close()

print " -> 1.2 Destination db"

conn = create_connection(config.destination_db, config.common_data)

mapper = Mapper(conn, config.destination_db['db'], config, MiniLogger())
mapper.execute_preprocess_queries_target()
destination_db_map = mapper.map_db()

conn.commit()
conn.close()

print ""
print "STEP 2. Actually merge all the databases"
print ""

counter = 0
for source_db in config.merged_dbs:
    if (source_db['db'] != config.main_db):
        counter = counter + 1

    try:
        source_db_tpl = copy.deepcopy(config.common_data)
        source_db_tpl.update(source_db)
  
        destination_db_tpl = copy.deepcopy(config.common_data)
        destination_db_tpl.update(config.destination_db)

        merger = Merger(destination_db_map, source_db_tpl, destination_db_tpl, config, counter, MiniLogger())
        merger.merge()
  

    except Exception,e:
        conn = merger._conn if globals().has_key('merger') else None
        handle_exception("There was an unexpected error while merging db %s" % source_db['db'], e, conn)

print ""
print "STEP 3. Performing post-processing"
print ""

conn = create_connection(config.destination_db, config.common_data)

mapper = Mapper(conn, config.destination_db['db'], config, MiniLogger())
mapper.execute_postrocess_queries_target()

conn.commit()
conn.close()

print "Merge is finished"
