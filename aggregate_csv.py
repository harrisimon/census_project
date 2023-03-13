import pandas as pd
pd.set_option('display.max_columns', None)
# figuring out how to map division and region to existing state columns

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
# print(census_df.count())

###################################
# MERGE current_state WITH region
###################################
# merge census df with states df
def merge_region(df):
    current_state = df.merge(states, left_on='current_state', right_on='abbrv')
    current_state.drop(['level', 'abbrv'], axis=1, inplace=True)
    # MERGE current_state to division
    curr_state_division = current_state.merge(divisions, left_on='parent_id', right_on='id')
    curr_state_division.drop(['id_y', 'abbrv', 'parent_id_x'], axis=1, inplace=True)
    # MERGE current_divison to region
    curr_region = curr_state_division.merge(regions, left_on='parent_id_y', right_on='id')
    curr_region.drop(['abbrv', 'parent_id', 'level_y', 'name_y', 'id_x', 'name_x'], axis=1, inplace=True)
    curr_region = curr_region.rename(columns={'name': 'current_region'})
    return  curr_region
regions = merge_region(census_df)
###################################
# MERGE previous division
###################################
def merge_division(df):

    previous_division = df.merge(states, left_on='previous_state', right_on='abbrv')
    previous_division.drop(['id_y', 'name', 'abbrv', 'level'], axis=1, inplace=True)
    previous_division = previous_division.merge(divisions,  left_on='parent_id', right_on='id')
    drop_terms = ['id', 'abbrv', 'level_x', 'level', 'parent_id_x', 'parent_id_y', 'id_x']
    previous_division.drop(drop_terms, axis=1, inplace=True)
    previous_division = previous_division.rename(columns={'name': 'previous_division'})
    return previous_division
# print(previous_division)

divisions = merge_division(regions)

# previous_division.to_csv('./exported_csvs/agg.csv', encoding='utf-8', index=False)
# print(previous_division.count())
###################################
# GROUP BY
###################################
cols = ['current_region', 'previous_division','estimate']
group_by_items = ['previous_division', 'current_region', 'year']
aggregate_migration = divisions.groupby(group_by_items)[cols].sum(numeric_only=True)


print(aggregate_migration)
aggregate_migration.to_csv('./exported_csvs/aggregated_migration.csv', encoding='utf-8', index=True)





