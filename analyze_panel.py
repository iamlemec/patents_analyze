# firm panel analysis

import sqlite3

# constants
fname_db = 'store/patents_new.db'

min_year = 1985
max_year = 2005
year_bins = [1985, 1990, 1995, 2000]

id_cols = ['firm_num', 'year']
sql_cols = ['assets', 'capx', 'cash', 'cogs', 'deprec', 'intan', 'debt', 'employ', 'income', 'revenue', 'sales', 'rnd', 'fcost', 'mktval', 'acquire', 'file_pnum', 'grant_pnum', 'source_pnum', 'dest_pnum', 'expire_pnum', 'n_cited', 'n_citing']
var_cols = ['active'] + sql_cols
var_period = [f'{col}_period' for col in var_cols]
var_total = [f'{col}_total' for col in var_cols]

# useful functions
incr = lambda x: x + 1

# merge map function
def merge_func(df, idx, func, id_cols=[], var_cols=None, suffix='_next', drop=True):
    # add in mapped index
    idx_next = f'{idx}_merge_func'
    df1 = df.copy()
    df1[idx_next] = df[idx].apply(func)

    # do merge
    right_cols = var_cols + id_cols + [idx]
    df2 = df1.merge(df[right_cols], how='left', left_on=id_cols+[idx_next], right_on=id_cols+[idx], suffixes=('', suffix))

    # drop merge column
    if drop:
        df2 = df2.drop(idx_next, axis=1)

    return df2

# open db
con = sqlite3.connect(fname_db)

# load data for desired years and columns
col_str = ','.join(id_cols+sql_cols)
datf_year = pd.read_sql(f'select {col_str} from firmyear_index where year >= {min_year} and year < {max_year}', con)
datf_year['active'] = 1 # sentinel

# tabulate next years values
datf_year = merge_func(datf_year, 'year', incr, id_cols=['firm_num'], var_cols=var_cols)

# tabulate lifetime totals
firm_groups = datf_year.groupby('firm_num')
firm_sums = firm_groups[var_cols].sum()
datf_year = datf_year.join(firm_sums.add_suffix('_total'), on='firm_num')

# tabulate five year totals
datf_year['period'] = np.digitize(datf_year['year'], year_bins)
period_groups = datf_year.groupby(['firm_num', 'period'])
datf_period = period_groups[var_cols].sum().reset_index()
datf_period = merge_func(datf_period, 'period', incr, id_cols=['firm_num'], var_cols=var_cols)
datf_period = datf_period.join(firm_sums.add_suffix('_total'), on='firm_num')
datf_period = datf_period.fillna(0)