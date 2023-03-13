import pandas as pd
import numpy as np

census_file_path = './census_migration_data.csv'
nc_df = pd.read_csv(census_file_path)
state_pop_file_path = './state_populations.csv'
census_pop_df = pd.read_csv(state_pop_file_path)
classification_path = './census_classification.csv'
classification_df = pd.read_csv(classification_path)
regions = classification_df[classification_df['level'] == 'region']
divisions = classification_df[classification_df['level'] == 'division']
states = classification_df[classification_df['level'] == 'state']

nc_df = nc_df.loc[nc_df['current_state'] == 'NC']
nc_df.drop('current_state', axis=1, inplace=True)
# print(nc_df)
w_region = nc_df.merge(states, left_on='previous_state', right_on='abbrv')
w_region.drop(['abbrv','name', 'id', 'level'], axis=1, inplace=True)
region = w_region.merge(divisions, left_on='parent_id', right_on='id')
region.drop(['abbrv', 'level','id', 'parent_id_y', 'parent_id_x'], axis=1,inplace=True)
print(region)
region.to_csv('./exported_csvs/nc_graphql_data.csv', encoding='utf-8', index=False)
