def job_feature (rpt_dt):
    #creating tables
    tbl_nm = 'CINS_FEATURE_STORE_TEST'
    try:
        create_tbl = open('script_create_tbl.txt','r')
        query_create = create_tbl.read()
    except:
        print("Unable to read Create Table text file")
        conn.close()
        sys.exit()
        create_tbl.close()
    n = 0
    print('--------STARTING CREATING TABLE(S)--------')
    for sql_create_tbl in query_create.split(';'):
        #sql_create_tbl = sql_create_tbl.replace("{SRT_DT}",srt_dt)
        sql_create_tbl = sql_create_tbl.replace("{RPT_DT}",rpt_dt)
        #sql_create_tbl = sql_create_tbl.replace("{END_DT}",end_dt)
        n = n + 1
        print(sql_create_tbl)
        try:
            cur.execute(sql_create_tbl)
            conn.commit()
            print('-----Finished creating ' + str(n) + ' table(s)-----')
        except:
            print("Unable to create table(s)")
            conn.close()
            sys.exit()
    #insert features
    try:
        insert = open('script_insert.txt','r') 
        query_insert = insert.read()
    except:
        print("Unable to read Insert Feature text file")
        conn.close()
        sys.exit()    
        insert.close()
    m = 0
    print('--------STARTING INSERTING FEATURE(S)--------')
    #delete 
    sql_del = f"DELETE FROM {tbl_nm} WHERE RPT_DT = '{rpt_dt}'"
    try:
        print(sql_del)
        cur.execute(sql_del)
        conn.commit()
        print(f'Sucessfully deleted data in {tbl_nm} with report date = {rpt_dt}')
    except:
        print("Unable to delete feature(s)")
        conn.close()
        sys.exit()
        drop.close()        
    for sql_insert in query_insert.split(';'):
        #sql_insert = sql_insert.replace("{SRT_DT}",srt_dt)
        sql_insert = sql_insert.replace("{RPT_DT}",rpt_dt)
        #sql_insert = sql_insert.replace("{END_DT}",end_dt)
        sql_insert = sql_insert.replace("{TBL_NM}",tbl_nm)
        m = m + 1
        print(sql_insert)
        try:
            cur.execute(sql_insert)
            conn.commit()
            print('-----Finished inserting ' + str(m) + ' feature(s)-----')
        except:
            print("Unable to insert feature(s)")
            conn.close()
            sys.exit()
    #drop tmp tables
    try:
        drop = open('script_drop_tbl.txt','r') 
        query_drop = drop.read()
    except:
        print("Unable to read Drop Table text file")
        conn.close()
        sys.exit()
        drop.close()
    t = 0
    print('--------STARTING DROPING TABLE(S)--------')
    for sql_drop in query_drop.split(';'):
        #sql_drop = sql_drop.replace("{SRT_DT}",srt_dt)
        sql_drop = sql_drop.replace("{RPT_DT}",rpt_dt)
        #sql_drop = sql_drop.replace("{END_DT}",end_dt)
        t = t + 1
        print(sql_drop)
        try:
            cur.execute(sql_drop)
            conn.commit()
            print('-----Finished dropping ' + str(t) + ' table(s)-----')
        except:
            print("Unable to drop table(s)")
            conn.close()
            sys.exit()
        print('DONE!!!')
    #close files and connection
    create_tbl.close()
    insert.close()
    drop.close()
    conn.close()
    sys.exit()
    
    
    
    
    
