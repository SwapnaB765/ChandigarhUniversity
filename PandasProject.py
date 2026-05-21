import pandas as pd
df = pd.read_csv('Project.csv')
df = df.set_index(['Ward No','Owner Name'])
df = df.sort_index('Ward No')

#for df in pd.read_csv('Project.csv', chunksize=100):
print(df)

