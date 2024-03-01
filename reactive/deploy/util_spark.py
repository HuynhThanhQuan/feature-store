from pyspark.sql import SparkSession
import os


spark = SparkSession.builder.appName("ETL").config("spark.executor.extraJavaOptions","-Dhttps.proxyHost=http://proxy365.sacombank.com -Dhttps.proxyPort=1985").getOrCreate()


def download_to_parquet(query, path):
    df = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@192.168.124.204:1521/X7DWDEV").option("driver","oracle.jdbc.driver.OracleDriver").option("user","CINS_ADS").option("password","Oracle#123").option("fetchsize", 100000).option("query",query).load()
    df.write.parquet(path)


def download_or_reload(path, query, reload_local_file=True):
    if (not os.path.exists(path)) or (not reload_local_file):
        return download_to_parquet(path, query)
    else:
        return reload(path)


def reload(path):
    df = pd.read_parquet(path)
    logger.debug(f'{path}: reloaded - len {len(df)}')
    return df