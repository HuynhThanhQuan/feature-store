from connect import oraDB
conn, cur = oraDB.connect_CINS_SMY()


def download_or_reload(saved_fn, query, cursor=cur):
    if not os.path.exists(saved_fn):
        download_to_parquet(saved_fn, query, cursor)
    df = reload(saved_fn)
    return df


def download_to_parquet(saved_fn, query, cursor=cur):
    df = load_sql_to_dataframe(query, cursor=cur)
    df.to_parquet(saved_fn)


def load_sql_to_dataframe(query, cursor=cur):
    cursor.execute(query)
    result = cursor.fetchall()
    column_names = [c[0] for c in cursor.description]
    df = pd.DataFrame(result, columns=column_names)
    return df

def reload(saved_fn):
    df = pd.read_parquet(saved_fn)
    return df