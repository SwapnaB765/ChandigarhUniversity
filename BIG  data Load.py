import pandas as pd

# c=0
for df in pd.read_csv('heroes.csv', chunksize=100):
    df = df.loc[df['Weight'] > 100]
    print(df)
#   c=c+1
# print(c)
