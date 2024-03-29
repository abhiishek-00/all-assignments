import os
import csv
import pandas as pd
#from transform import transform_data

data = []
input_folder = './logs'
output_file = 'collated.log'
output_csv = 'collated.csv'
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
                if not("#Software" in line or "#Version" in line or "#Date" in line or "#Fields" in line):
                    fpOut.write("{}\n".format(line))
                    #print(line)
                    #data.append(line)
                    #print(data)

df = pd.read_csv('collated.log', sep='\s', engine='python', on_bad_lines = 'warn')
df.to_csv('extracted.csv', index=None)
print(list(df.columns.values))
