import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

apps_with_duplicate = pd.read_csv('google_play_apps.csv')
# Dropping duplicate
apps = apps_with_duplicate.drop_duplicates()
# print total number of apps
# n = 5
# print(apps.sample(n))
# Data cleaning
char = [',', '$', '+']

# List of column names to clean
col = ['Installs', 'Price']

# Loop for each column
for i in col:
    # Replace each character with an empty string
    for j in char:
        apps[i] = apps[i].str.replace(j, '')
    # Convert col to numeric
    apps[i] = pd.to_numeric(apps[i])
num_apps_in_category = apps['Category'].value_counts()

sns.relplot(x=num_apps_in_category.index, y=num_apps_in_category.values, data=num_apps_in_category )
plt.grid()

plt.show()
