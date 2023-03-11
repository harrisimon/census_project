import pandas as pd

# figuring out how to map division and region to existing state columns

# print(aggregate_migration_data('./census_migration_data.csv'))

# filepaths
census_file_path = './census_migration_data.csv'
classification_file_path = './census_classification.csv'
# read files
census_df = pd.read_csv(census_file_path)
classification_df = pd.read_csv(classification_file_path)
# break out regions, divisions, and states
regions = classification_df[classification_df['level'] == 'region']
divisions = classification_df[classification_df['level'] == 'division']
states = classification_df[classification_df['level'] == 'state']

# drop margin of error
census_df.drop('margin_of_error', axis=1, inplace=True)


# MERGE current_state WITH region
current_state_division = census_df.merge(states, left_on='current_state', right_on='abbrv')
current_state_division.drop(['level', 'abbrv'], axis=1, inplace=True)
# print(current_state_division.head())
# MERGE current_state to division
curr_state_division = current_state_division.merge(divisions, left_on='parent_id', right_on='id')
curr_state_division.drop(['id_y', 'abbrv', 'parent_id_x'], axis=1, inplace=True)
# print(current_state_division.head())
# MERGE current_divison to region
curr_division = curr_state_division.merge(regions, left_on='parent_id_y', right_on='id')
curr_division.drop(['abbrv', 'parent_id', 'level_y', 'name_y', 'id_x', 'name_x'], axis=1, inplace=True)

curr_division = curr_division.rename(columns={'name': 'current_region'})
print(curr_division.head())

# MERGE previous division
previous_division = curr_division.merge(states, left_on='previous_state', right_on='abbrv')
previous_division.drop(['id_y', 'name', 'abbrv', 'level'], axis=1, inplace=True)
previous_division = previous_division.merge(divisions,  left_on='parent_id', right_on='id')
previous_division.drop(['id', 'abbrv', 'level_x', 'level', 'parent_id_x', 'parent_id_y'], axis=1, inplace=True)
previous_division = previous_division.rename(columns={'name': 'previous_division'})
previous_division.to_csv('merge4.csv', encoding='utf-8', index=False)
print(previous_division)

# group = curr_state_division_match.groupby(['parent_id_y', 'year'])
# print(group)
# merge previous_state to region
# previous_region_merge = current_state_division.merge(states(''))

# division_merge.to_csv('merge1.csv', encoding='utf-8', index=False)


# classification_df.set_index('abbrv', inplace=True)
# census_df['current_division'] = census_df['current_state'].map(classifcation_df['level'])
# print(census_df)
# print(states)
# print(divisions)
print(regions)
