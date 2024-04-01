import os
import pandas as pd

def extractData():
    data = []
    input_folder = '/opt/airflow/dags/logfiles'
    output_file = '/opt/airflow/dags/collated.log'
    output_csv = '/opt/airflow/dags/collated.csv'
    fpOut = open(output_file, 'w')
    header = "date time s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs(User-Agent) sc-status sc-substatus sc-win32-status time-taken"
    fpOut.write("{}\n".format(header))

    for file in os.listdir(input_folder):
        if file.endswith(".log"):
            print("processing "+file)
            with open(os.path.join(input_folder, file), 'r') as f:
                lines = f.readlines()
                for line in lines:
                    line = line.rstrip()
                    if not("#Software" in line or "#Version" in line or "#Date" in line or "#Fields" in line or "/robots.txt" in line):
                        fpOut.write("{}\n".format(line))

    df = pd.read_csv('/opt/airflow/dags/collated.log', sep='\s', engine='python', on_bad_lines = 'warn')
    df.to_csv('/opt/airflow/dags/extracted.csv', index=None)
    print(list(df.columns.values))

extractData()
