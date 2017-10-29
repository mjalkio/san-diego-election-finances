import os.path as op

import pandas as pd

YEARS = range(2007, 2017)
RELEVANT_COLUMNS = ['corrected_name',
                    'contribution_amount',
                    'contributor_first',
                    'contributor_last',
                    'report_year',
                    'candidate_or_measure',
                    'candidate_office_sought',
                    'support_or_oppose']


def contributor_name(row):
    first = row['contributor_first']
    last = row['contributor_last']
    return ' '.join([first, last])

fin_recipients_path = op.join('..', '..', 'data', 'raw', 'financial_support_recipients_datasd.csv')
fin_recipients_df = pd.read_csv(fin_recipients_path)

# Some recipients support multiple measures at the same time.
# Dropping like this messes things up, but I couldn't think of a cleaner way.
fin_recipients_df = fin_recipients_df.drop_duplicates(subset=('year_reported', 'recipient_name'))

fin_support_df = pd.DataFrame()
for year in YEARS:
    filename = op.join('..', '..', 'data', 'raw',
                       "financial_support_{year}_datasd.csv".format(year=year))
    fin_support_year_df = pd.read_csv(filename)
    fin_recipients_year_df = fin_recipients_df[fin_recipients_df['year_reported'] == year]
    fin_support_year_full_df = pd.merge(fin_support_year_df, fin_recipients_year_df,
                                        on='recipient_name')
    fin_support_df = fin_support_df.append(fin_support_year_full_df, ignore_index=True)

fin_support_df = fin_support_df[fin_support_df['contribution_amount'] > 0]
fin_support_df = fin_support_df[RELEVANT_COLUMNS]
fin_support_df = fin_support_df.fillna('')
fin_support_df = fin_support_df.assign(contributor_name=fin_support_df.apply(contributor_name,
                                                                             axis='columns'))
fin_support_df = fin_support_df.drop(columns=['contributor_first', 'contributor_last'])
fin_support_df = fin_support_df.rename(columns={'corrected_name': 'recipient_name',
                                                'report_year': 'year'})

interim_data_path = op.join('..', '..', 'data', 'interim', 'fin_support_data.csv')
fin_support_df.to_csv(interim_data_path)
