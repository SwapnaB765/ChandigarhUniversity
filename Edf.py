# empty data frame
import pandas as pd

edf = pd.DataFrame()
# c=0
for df in pd.read_csv('heroes.csv', chunksize=100):
    df = df.loc[df['Weight'] > 100]
    edf = pd.concat([edf, df])
#   c=c+1
# print(c)
edf.to_excel(r'C:\Users\Admin\OneDrive\Desktop\edf.xlsx')
edf.reset_index(False)
print(edf)
