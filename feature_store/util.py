import time
from functools import wraps
import logging
from oraDB import oraDB

logger = logging.getLogger(__name__)

def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.debug(f'[timeit] Function "{func.__name__}" took "{elapsed_time}" seconds')
        return result
    return wrapper


@timeit
def my_func(n=100):
    for _ in range(n):
        pass
    
def get_numrow_from_insert():
    conn, cur= oraDB.connect()
    
    # Check exisiting table
    query = """SELECT table_name FROM user_tables WHERE table_name = 'CINS_TMP_UTIL_TEST'"""
    cur.execute(query)
    result = cur.fetchone()
    conn.commit()
    if result is not None:
        print('Exist')
        print('Drop table')
        query = """DROP TABLE CINS_TMP_UTIL_TEST"""
        cur.execute(query)
        conn.commit()
    else:
        print('Not exists')
        
    query = """CREATE TABLE CINS_TMP_UTIL_TEST (CUSTOMER_CDE VARCHAR2(25 BYTE))"""
    cur.execute(query)
    conn.commit()
    print('Created')
    
    query = """INSERT INTO CINS_TMP_UTIL_TEST (CUSTOMER_CDE) VALUES (123)"""
    cur.execute(query)
    result = cur.rowcount
    conn.commit()
    if result is not None:
        print(result)
    
if __name__ == '__main__':    
    get_numrow_from_insert()
