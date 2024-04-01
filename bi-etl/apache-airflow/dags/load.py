import psycopg2

def loadTables(tablename, createQuery, csv_file_path, cur):
    # drop table if exists
    cur.execute('DROP TABLE IF EXISTS '+ tablename + ' cascade')
    cur.execute(createQuery)
    # copy data from csv to the table
    copyQuery = f"""
        COPY {tablename} FROM stdin 
        DELIMITER as ','
        CSV HEADER;
    """
    with open(csv_file_path, 'r') as f:
        cur.copy_expert(sql=copyQuery, file=f)

def loadData():
    dbvalues = {
        "dbname":"reema",
        "user":"reema",
        "password":"reema123"
    }

    # connect to DB
    connectionString = "host=postgres-sql-db dbname=" + dbvalues['dbname'] + " user=" + dbvalues['user'] + " password=" + dbvalues['password']
    conn = psycopg2.connect(connectionString)
    cur = conn.cursor()
    # set automatic commit to be true, so that each action is committed without having to call conn.commit() after each command
    conn.set_session(autocommit=True)

    # create table projectetl
    tablename = 'projectetl'
    csv_file_path = '/opt/airflow/dags/transformed.csv'
    createQuery = 'CREATE TABLE '+ tablename + "("
    createQuery += """
            id int,
            year int,
            quarter text,
            month text,
            week date,
            day text,
            hour int,
            ip text,
            country text,
            city text,
            uri text,
            filetype text,
            os text,
            browser text,
            statuscode int,
            timetaken int,
            PRIMARY KEY (id)
        )
        """
    loadTables(tablename, createQuery, csv_file_path, cur)

    # create relational tables
    tablename = 'etldatetable'
    csv_file_path = '/opt/airflow/dags/etldatetable.csv'
    createQuery = 'CREATE TABLE '+ tablename + "("
    createQuery += """
            id text,
            year int,
            quarter text,
            month text,
            week date,
            day text,
            hour int,
            PRIMARY KEY (id)
        )
        """
    loadTables(tablename, createQuery, csv_file_path, cur)
    tablename = 'eltclienttable'
    csv_file_path = '/opt/airflow/dags/eltclienttable.csv'
    createQuery = 'CREATE TABLE '+ tablename + "("
    createQuery += """
            id text,
            ip text,
            country text,
            city text,
            PRIMARY KEY (id)
        )
        """
    loadTables(tablename, createQuery, csv_file_path, cur)
    tablename = 'etlrequesttable'
    csv_file_path = '/opt/airflow/dags/etlrequesttable.csv'
    createQuery = 'CREATE TABLE '+ tablename + "("
    createQuery += """
            id text,
            uri text,
            filetype text,
            os text,
            browser text,
            PRIMARY KEY (id)
        )
        """
    loadTables(tablename, createQuery, csv_file_path, cur)
    tablename = 'etlmaintable'
    csv_file_path = '/opt/airflow/dags/etlmaintable.csv'
    createQuery = 'CREATE TABLE '+ tablename + "("
    createQuery += """
            id int,
            statuscode int,
            timetaken int,
            dateid text references etldatetable(id),
            clientid text references eltclienttable(id),
            requestid text references etlrequesttable(id),
            PRIMARY KEY (id)
        )
        """
    loadTables(tablename, createQuery, csv_file_path, cur)

    # commit the changes and close the connection to the default database
    conn.commit()
    cur.close()
    conn.close()
