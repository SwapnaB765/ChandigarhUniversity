import pandas as pd
import numpy as np
df = pd.read_csv('pokemon.csv')
df=df.groupby('Type 1')[['Speed','Attack']].agg([np.max,np.min])

print(df)

