import pandas as pd
import numpy as np
df=pd.read_csv('date22.csv')
df['Date1']=pd.to_datetime(df['Date1'])
df['Date2']=pd.to_datetime(df['Date2'])
df['diff']=df['Date1']-df['Date2']
df['new']=df['diff']/np.timedelta64(2,'Y')

print(df)
