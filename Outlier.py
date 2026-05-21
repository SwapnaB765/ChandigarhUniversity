import pandas as pd
import numpy as np
df = pd.read_csv('walmart.csv')
print(df)
q1,q3 = np.percentile(df['Weekly_Sales'],[25,75])
iqr = q3-q1
lb = q1 - (1.5 * iqr)
ub = q3 + (1.5 * iqr)
ofdf = df.loc[(df['Weekly_Sales']>=lb) & (df['Weekly_Sales']<=ub)]
print(ofdf)
