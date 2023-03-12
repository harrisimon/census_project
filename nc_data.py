import numpy as np
import pandas as pd

# filepaths etc
census_file_path = './census_migration_data.csv'
nc_df = pd.read_csv(census_file_path)
state_pop_file_path = './state_populations.csv'
census_pop_df = pd.read_csv(state_pop_file_path)
classification_path = './census_classification.csv'
classification_df = pd.read_csv(classification_path)
regions = classification_df[classification_df['level'] == 'region']
divisions = classification_df[classification_df['level'] == 'division']
states = classification_df[classification_df['level'] == 'state']
# print(classification_df)


nc_df = nc_df.loc[nc_df['current_state'] == 'NC']
# print(nc_df)
nc_df.to_csv('./exported_csvs/nc.csv', encoding='utf-8', index=False)
nc_df.drop('current_state', axis=1, inplace=True)
# prev_state = nc_df.sort_values(by=['estimate'], ascending=False)

#######################################
## GET STATE W/ LARGEST # PEOPLE MOVING
#######################################
def largest_prev_state(df):
    prev_state_max = nc_df.loc[nc_df.groupby('year')['estimate'].idxmax()]
    return prev_state_max

# print(largest_prev_state(nc_df))
# nc_prev_states_max.to_csv('./exported_csvs/largest_prev_state.csv', encoding='utf-8', index=False)
#######################################
## GET STATE W/ LARGEST PROPORTION MOVING
#######################################
def largest_proportion(state_df, pop_df):
    # getting regions
    pop_merge_df = pd.merge(state_df, pop_df, how='left', left_on=['year', 'previous_state'], right_on=['year', 'state'])
    pop_merge_df.drop('state', axis=1, inplace=True)
    compare = pop_merge_df['estimate'] == pop_merge_df['population']
    pop_merge_df['proportion'] = np.where(compare, 0, pop_merge_df['estimate']/pop_merge_df['population'])
    proportion = pop_merge_df.loc[pop_merge_df.groupby('year')['proportion'].idxmax()]
    return proportion

#######################################
## NUM OF STATES WITH MORE THAN 10K MOVING
#######################################
def more_than_ten_k(df):
    filter = nc_df['estimate'] >= 10000
    states = nc_df[filter]
    count = states.groupby('year').count()
    single_count = count['previous_state']
    return single_count
# print(more_than_ten_k(nc_df))

#######################################
## PERCT OF PEOPLE OUTSIDE OF SA DIV
#######################################
def get_percentage(df):
    prev_abbrv = df.merge(states, left_on='previous_state', right_on='abbrv')
    prev_abbrv.drop(['level', 'abbrv', 'name'], axis=1, inplace=True)
    prev_div = prev_abbrv.merge(divisions, left_on='parent_id', right_on='id')
    prev_div.drop(['id_y', 'abbrv', 'parent_id_x', 'id_x', 'level', 'parent_id_y'], axis=1, inplace=True)
    prev_div = prev_div.rename(columns={'name': 'previous_division'})
    total_cols = ['estimate','year','previous_division']
    total_nc_migrate = prev_div[total_cols].groupby('year')[
        ['previous_division', 'estimate']].sum(numeric_only=True)
    not_s_a = prev_div['previous_division'] != 'South Atlantic'
    exclude = prev_div[not_s_a]
    exclude_cols = ['estimate', 'year', 'previous_division']
    total_non_sa = exclude[exclude_cols].groupby('year')[
        ['previous_division', 'estimate']].sum(numeric_only=True)
    comparison = total_nc_migrate['estimate'] == total_non_sa['estimate']
    perc = pd.DataFrame(np.where(comparison, 0, total_non_sa['estimate']/total_nc_migrate['estimate']))
    perc = perc.rename(columns={0: '% outside of SA'})
    return perc


#######################################
## CREATE CSV
#######################################
col1 = largest_prev_state(nc_df)
col1 = col1.rename(columns={'previous_state':'largest_migrant_state'})
# print(col1[['year', 'largest_migrant_state']])
col1 = col1[['year', 'largest_migrant_state']]
col1.reset_index(drop=True, inplace=True)
# print(col1)
years = col1['year']
migrant_pop = col1['largest_migrant_state']
col2 = largest_proportion(nc_df, census_pop_df)
col2 = col2.rename(columns={'previous_state':'largest_migrant_proportion'})
col2 = col2['largest_migrant_proportion']
col2.reset_index(drop=True, inplace=True)
# print(col2)

col3 = more_than_ten_k(nc_df)
col3 = col3.rename('# of states with over 10k')
col3.reset_index(drop=True, inplace=True)
# print(col3)
col4 = get_percentage(nc_df)
# print(col4)
combined = col1.merge(col2, left_index=True, right_index=True)
combined = combined.merge(col3, left_index=True, right_index=True)
# print(combined)
combined = combined.merge(col4, left_index=True, right_index=True)
combined.to_csv('./exported_csvs/nc_migration.csv', encoding='utf-8', index=False)
print(combined)







