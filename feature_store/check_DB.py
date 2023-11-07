from oraDB import oraDB
import cx_Oracle
# Set up logging
import logging
import datetime
import util

logger = logging.getLogger(__name__)


@util.timeit
def check_exists(response):
    logger.info('Checking existing TMP tables')
    tables = response['RPT_TABLE']
    map_check = {}
    conn,cur= oraDB.connect()
    for t in tables:
        cur.execute("select table_name FROM user_tables WHERE table_name = :tbl", {'tbl': t})
        result = cur.fetchall()
        if len(result) > 0:
            map_check[t] = True
        else:
            map_check[t] = False
    cur.close()
    conn.close()
    
    # Log check info
    notexist_tables = []
    for k, v in map_check.items():
        if not v:
            notexist_tables.append(k)
    if len(notexist_tables) == 0:
        logger.info('All TMP tables exist')
    else:
        logger.warn(f'Missing {len(notexist_tables)} TMP tables: {notexist_tables}')
        logger.warn('Need to fulfill TMP tables first in order to execute next')
    return {'TABLE_CHECK': map_check}

@util.timeit
def drop_tables(response):
    table_check = response['TABLE_CHECK']
    conn,cur= oraDB.connect()
    drop_tables = []
    logging.info(f'Tables to be dropped {list(table_check.keys())}')
    for k, v in table_check.items():
        try:
            if v:
                cur.execute(f"DROP TABLE {k}")
                cur.execute('COMMIT')
                drop_tables.append(k)
                logging.info(f'Dropped table {k}')
        except cx_Oracle.DatabaseError as e:
            logger.error(e)
            logger.error(f'Failed to drop table {k}')
        except Exception as error:
            logger.error(error)
    logger.info('Dropped tmp tables completed')
    cur.close()
    conn.close()
    return {'DROPPED_TABLES': drop_tables}
