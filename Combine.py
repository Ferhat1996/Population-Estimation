import pandas as pd

# Load data
pop = pd.read_csv("Population_England.csv", sep=';')

# Clean data
pop_clean = pop[(pop['year'] > 1200) & (pop['pop'] > 0)]
pop_clean = pop_clean.sort_values(by=['Pop_ID', 'year'])
pop_clean_max = pop_clean.groupby(['Pop_ID', 'year'])['pop'].max().reset_index()

# Transfer data
transfer = pd.read_csv("Transfer_Pop_CCED.csv", sep=";")
transfer = transfer.drop_duplicates(subset=['pop_id', 'cced_id'], keep='first')

# Merge data
merged = pd.merge(pop_clean_max, transfer, left_on='Pop_ID', right_on='pop_id')

# Reshape merged data
final = merged.melt(id_vars=['pop_id', 'cced_id', 'year'], value_vars=['pop'], var_name='pop_type', value_name='population')
final = final.sort_values(by=['cced_id', 'year'])

# Save data
final.to_csv("Final/Pop_CCED.csv", index=False)
final.to_pickle("Final/Pop_CCED.pkl")
