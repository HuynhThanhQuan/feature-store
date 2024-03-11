import cx_Oracle
from sqlalchemy import create_engine
import pandas as pd
import os

import logging
logger = logging.getLogger(__name__)

os.environ['HTTPS_PROXY'] = "http://proxy365.sacombank.com:1985"

cx_Oracle.init_oracle_client()

class oraDB:
    def connect_CINS_SMY():
        # engine = create_engine(f"oracle://CINS_SMY:Oracle#123@192.168.124.204:1521/?service_name=X7DWDEV")
        # connection = engine.connect()
        # return engine, connection

        host='192.168.124.204'
        port= 1521
        sname='X7DWDEV'
        user='CINS_SMY'
        password='Oracle#123'

        connection = cx_Oracle.connect('{}/{}@{}:{}/{}'.format(user, password, host, str(port), sname))
        cursor = connection.cursor()
        return connection, cursor

    def connect_DW_ANALYTICS():
        # engine = create_engine(f"oracle://DW_ANALYTICS:N3wp@ss123@192.168.124.204:1521/?service_name=X7DWDEV")
        # connection = engine.connect()
        # return engine, connection

        host='192.168.124.204'
        port= 1521
        sname='X7DWDEV'
        user='DW_ANALYTICS'
        password='N3wp@ss123'

        connection = cx_Oracle.connect('{}/{}@{}:{}/{}'.format(user, password, host, str(port), sname))
        cursor = connection.cursor()
        return connection, cursor
        