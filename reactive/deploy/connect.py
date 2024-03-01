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
        engine = create_engine(f"oracle://CINS_SMY:Oracle#123@192.168.124.204:1521/?service_name=X7DWDEV")
        connection = engine.connect()

        return engine, connection