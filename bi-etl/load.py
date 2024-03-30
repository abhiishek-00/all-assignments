import psycopg2

dbvalues = {
    "dbname":"",
    "user":"",
    "password":""
}

tablename = 'projectetl'
csv_file_path = 'transformed.csv'

# connect to DB
connectionString = "host=localhost dbname=" + dbvalues['dbname'] + " user=" + dbvalues['user'] + " password=" + dbvalues['password']
conn = psycopg2.connect(connectionString)
cur = conn.cursor()
# set automatic commit to be true, so that each action is committed without having to call conn.commit() after each command
conn.set_session(autocommit=True)
#cur.execute('SELECT * FROM inventory')
#print(cur.fetchone())

# drop table if exists
cur.execute('DROP TABLE IF EXISTS '+ tablename)
# create table with columns & add id as a primary key
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
cur.execute(createQuery)
#cur.execute('SELECT * FROM projectetl')

# copy data from csv to the table
copyQuery = f"""
    COPY {tablename} FROM stdin 
    DELIMITER as ','
    CSV HEADER;
"""
with open(csv_file_path, 'r') as f:
    cur.copy_expert(sql=copyQuery, file=f)
#cur.execute('SELECT * FROM projectetl')

# Commit the changes and close the connection to the default database
conn.commit()
cur.close()
conn.close()
