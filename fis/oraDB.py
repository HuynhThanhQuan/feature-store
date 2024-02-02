class oraDB:
    def __init__(self, conn, cur):
        self.conn=conn
        self.cur=cur
        
    def connect():
        import cx_Oracle
        import time
        import warnings
        warnings.filterwarnings('ignore')
        import logging
        logger=logging.getLogger()
        logger.disable=True

        ##### thong tin ket noi voi du an FIS
        host='192.168.124.100'
        port= 1521
        sname='DWDEV'
        user='CINS_SMY'
        password='Oracle#123'

        start=time.time()

        ### connect to DB
        try:
            conn=cx_Oracle.connect('{}/{}@{}:{}/{}'.format(user,password,host,str(port),sname))
            cur=conn.cursor()
            cur.execute('ALTER SESSION FORCE PARALLEL DML PARALLEL 16')
            cur.execute('ALTER SESSION FORCE PARALLEL QUERY PARALLEL 16')
            end=time.time()-start
            #print("--- % seconds ---" % end)
            #print("Connection successful!")
        except Exception as e:
            print("ErrMsg: "+str(e))
            print("Connect to DB failed!")

        return conn, cur
