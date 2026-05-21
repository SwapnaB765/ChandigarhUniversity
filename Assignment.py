import pandas as pd
from afinn import Afinn

data = pd.read_csv('user_reviews (1).csv')
data = data.iloc[:, 0:2]
data = data.dropna()
data = data.head(2000)


def sa(r):
    score = Afinn().score(r)
    if (score > 0):
        return 'Positive'
    elif (score < 0):
        return 'Negative'
    else:
        return 'Neutral'


data['sent'] = 'score'

data['sent'] = data['Translated_Review'].apply(sa)

print(data)
