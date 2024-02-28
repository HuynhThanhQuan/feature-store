import cx_Oracle
import logging
logger = logging.getLogger(__name__)


class oraDB:
    def connect_CINS_ADS():
        host='192.168.124.204'
        port= 1521
        sname='X7DWDEV'
        user='CINS_ADS'
        password='Oracle#123'
        
        try:
            conn = cx_Oracle.connect('{}/{}@{}:{}/{}'.format(user, password, host, str(port), sname))
            cur = conn.cursor()
        except Exception as e:
            logger.error("ErrMsg: "+str(e))
            logger.error("Connect to DB failed!")
        return conn, cur
    
    def connect_CINS_SMY():
        host='192.168.124.204'
        port= 1521
        sname='X7DWDEV'
        user='CINS_SMY'
        password='Oracle#123'
        
        try:
            conn = cx_Oracle.connect('{}/{}@{}:{}/{}'.format(user, password, host, str(port), sname))
            cur = conn.cursor()
        except Exception as e:
            logger.error("ErrMsg: "+str(e))
            logger.error("Connect to DB failed!")
        return conn, cur