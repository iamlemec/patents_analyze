import sqlite3
import numpy as np
import pandas as pd

# state/country maps
state_map = {
    'ALABAMA': 'AL',
    'ALASKA': 'AK',
    'AMERICAN SAMOA': 'AS',
    'ARIZONA': 'AZ',
    'ARKANSAS': 'AR',
    'CALIFORNIA': 'CA',
    'COLORADO': 'CO',
    'CONNECTICUT': 'CT',
    'DELAWARE': 'DE',
    'DISTRICT OF COLUMBIA': 'DC',
    'FLORIDA': 'FL',
    'GEORGIA': 'GA',
    'GUAM': 'GU',
    'HAWAII': 'HI',
    'IDAHO': 'ID',
    'ILLINOIS': 'IL',
    'INDIANA': 'IN',
    'IOWA': 'IA',
    'KANSAS': 'KS',
    'KENTUCKY': 'KY',
    'LOUISIANA': 'LA',
    'MAINE': 'ME',
    'MARYLAND': 'MD',
    'MASSACHUSETTS': 'MA',
    'MICHIGAN': 'MI',
    'MINNESOTA': 'MN',
    'MISSISSIPPI': 'MS',
    'MISSOURI': 'MO',
    'MONTANA': 'MT',
    'NATIONAL': 'NA',
    'NEBRASKA': 'NE',
    'NEVADA': 'NV',
    'NEW HAMPSHIRE': 'NH',
    'NEW JERSEY': 'NJ',
    'NEW MEXICO': 'NM',
    'NEW YORK': 'NY',
    'NORTH CAROLINA': 'NC',
    'NORTH DAKOTA': 'ND',
    'NORTHERN MARIANA ISLANDS': 'MP',
    'OHIO': 'OH',
    'OKLAHOMA': 'OK',
    'OREGON': 'OR',
    'PENNSYLVANIA': 'PA',
    'PUERTO RICO': 'PR',
    'RHODE ISLAND': 'RI',
    'SOUTH CAROLINA': 'SC',
    'SOUTH DAKOTA': 'SD',
    'TENNESSEE': 'TN',
    'TEXAS': 'TX',
    'UTAH': 'UT',
    'VERMONT': 'VT',
    'VIRGIN ISLANDS': 'VI',
    'VIRGINIA': 'VA',
    'WASHINGTON': 'WA',
    'WEST VIRGINIA': 'WV',
    'WISCONSIN': 'WI',
    'WYOMING': 'WY'
}
states = list(state_map.values())

# load data
con = sqlite3.connect('store/patents.db')
# datf_idx = pd.read_sql('select * from firmyear_index',con)
# firm_info = pd.read_sql('select * from firm_life',con)
grant_info = pd.read_sql('select patnum,firm_num,fileyear,grantyear,state,country,classone,classtwo from patent_info',con)
trans_info = pd.read_sql('select patnum,source_fn,dest_fn,execyear,recyear,state,country from assignment_info where execyear!=\'\'',con)
con.close()

# drop non-us and weird states
grant_info['state'] = grant_info['state'].fillna('').apply(str.upper)
trans_info['state'] = trans_info['state'].fillna('').apply(str.upper)
trans_info['state'] = trans_info['state'].map(state_map)

grant_info = grant_info.ix[grant_info['state'].isin(states)].drop('country',axis=1)
trans_info = trans_info.ix[trans_info['state'].isin(states)].drop('country',axis=1)

# drop non-utility patents
trans_info = trans_info.ix[trans_info['patnum'].apply(lambda x: type(x) == np.int)]
trans_info['patnum'] = trans_info['patnum'].astype(np.int)
trans_info = trans_info.ix[trans_info['patnum']>=1000000]

# reindex transfers
trans_sort = trans_info.sort(['patnum','execyear'])
trans_sort = trans_sort.merge(grant_info[['patnum','state']],how='inner',on='patnum',suffixes=['','0'])
trans_sort = trans_sort.rename(columns={'state':'dest_state'})

# track transfers
same_ppn = trans_sort['patnum'] == trans_sort['patnum'].shift()
trans_sort['source_state'] = ''
trans_sort['source_state'].ix[same_ppn] = trans_sort['dest_state'].shift()
trans_sort['source_state'].ix[~same_ppn] = trans_sort['state0']
trans_sort = trans_sort.drop('state0',axis=1)

# store output
trans_sort.to_csv('export/trans_info.csv',index=False)
grant_info.to_csv('export/grant_info.csv',index=False)

