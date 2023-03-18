import pandas as pd
from pybaseball import statcast
from datetime import date
import calendar
from postgresql_class import PSQL
import random
import sys


start = date(2008, 1, 1)
end = date.today()
csv_file_name = '.statcast.csv'
statcast_db_name = 'statcast'
statcast_tbl_name = 'statcast_pitch'
print('enter PSQL username:')
user = input()
print(f'enter password for {user}:')
pw = input()


def generate_statcast_csv(start_date, end_date, filename):
    first_insert = True
    for y in range(start_date.year, end_date.year+1):
        for m in range(1, 13):
            if (y == end_date.year) & (m > end_date.month):
                break
            temp_beg = str(date(y, m, 1))
            temp_end = str(date(y, m, calendar.monthrange(y, m)[1]))
            df_temp = statcast(start_dt=temp_beg, end_dt=temp_end)
            if len(df_temp):
                df_temp = df_temp[::-1]  # reverses order of dataframe
                if first_insert:
                    df_temp.to_csv(path_or_buf=filename, header=True, mode='w', index=False)
                    first_insert = False
                else:
                    df_temp.to_csv(path_or_buf=filename, header=False, mode='a', index=False)


def create_statcast_db(dbname, psql):
    psql.create_new_db(dbname=dbname)


def create_tbl_from_csv_sample(filename, tbl_name, psql):
    n = sum(1 for line in open(filename)) - 1  # number of records in file (excluding header)
    s = 10000  # desired sample size
    skip = sorted(random.sample(range(1, n + 1), n - s))  # the 0-indexed header will not be included in the skip list
    temp_df = pd.read_csv(filename, skiprows=skip, low_memory=False)
    try:
        psql.create_tbl(dataframe=temp_df, tbl_name=tbl_name)
    except Exception as e:
        print('error: ', e)
        print('exception type: ', type(e))
        sys.exit(1)


def bulk_insert_csv_into_tbl(csv, tbl_name, psql):
    psql.bulk_insert_into_tbl_from_csv(filename=csv, tbl_name=tbl_name)


if __name__ == "__main__":

    # generate csv with entire dataset
    generate_statcast_csv(start_date=start, end_date=end, filename=csv_file_name)
    print('CSV generated')

    # create db
    old_db = PSQL(user=user, password=pw)
    create_statcast_db(dbname=statcast_db_name, psql=old_db)

    # create table from random sample
    # this is to prevent creating the table based on old data with empty columns or new data with empty columns
    new_db = PSQL(user=user, password=pw, database=statcast_db_name)
    create_tbl_from_csv_sample(filename=csv_file_name, tbl_name=statcast_tbl_name, psql=new_db)
    print('table created from random sample')

    # bulk insert csv into fresh table
    bulk_insert_csv_into_tbl(csv=csv_file_name, tbl_name=statcast_tbl_name, psql=new_db)

