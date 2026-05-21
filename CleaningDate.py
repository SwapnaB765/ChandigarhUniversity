import pandas as pd

df = pd.read_csv('testing.csv')

# FSO Number**********
# df['FSO Number']=df['FSO Number'].astype(str)


df['temp'] = df['FSO Number'].str.len()
df['temp2'] = df['FSO Number'].str.isdigit()
df = df.loc[(df.temp == 10) & (df.temp2 == True)]
df = df.drop(columns=['temp', 'temp2'])
df['FSO Number'] = pd.to_numeric(df['FSO Number'])
# *************FSO Number
# First Name


df['First Name'] = df['First Name'].str.strip()
df['fname'] = df['First Name']
df['fname'] = df['fname'].str.replace('.', '')
df['temp']=df['fname'].str.isalpha()
df=df.loc[df.temp==True]
df=


print(df)
