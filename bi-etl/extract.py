import os
import pandas as pd
import numpy as np

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
                if not("#Software" in line or "#Version" in line or "#Date" in line or "#Fields" in line or "/robots.txt" in line):
                    fpOut.write("{}\n".format(line))

df = pd.read_csv('collated.log', sep='\s', engine='python', on_bad_lines = 'warn')
# remove rows having blank data
df['time-taken'].replace('', np.nan, inplace=True)
df.dropna(subset=['time-taken'], inplace=True)
# convert to int
df['sc-status'] = df['sc-status'].astype(int)
df['time-taken'] = df['time-taken'].astype(int)
df.to_csv('extracted.csv', index=None)
print(list(df.columns.values))
print(df.head(2))
