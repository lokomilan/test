import psycopg2
import pandas as pd


def get_dataframe(conn, table_name, table_schema='hr_vacancies'):
    column_names_query = '''
        SELECT column_name
        FROM information_schema.columns
        WHERE
            table_schema = '{}'
            AND table_name = '{}'
    '''
    table_rows_query = '''
        SELECT *
        FROM {}.{}
    '''
    with conn.cursor() as cur:
        cur.execute(column_names_query.format(table_schema, table_name))
        column_names = [column[0] for column in cur.fetchall()]
        cur.execute(table_rows_query.format(table_schema, table_name))
        table_rows = cur.fetchall()
    return pd.DataFrame(table_rows, columns=column_names)


def get_fast_trade_counts_per_user(df, limit_seconds=60):
    df['duration'] = (df.close_time - df.open_time).dt.total_seconds()
    df['is_fast_trade'] = df.duration.between(0, limit_seconds, inclusive='left')
    return df\
        .groupby('login', as_index=False)\
        .agg({'is_fast_trade': 'sum'})\
        .rename(columns={'is_fast_trade': 'fast_trades_count'})\
        .sort_values('login')


def main():

    connection = psycopg2.connect(
        dbname='postgres',
        user='user_test',
        password='qwerFD21',
        host='test-task-rto.c5qems882moh.ap-southeast-1.rds.amazonaws.com',
        port='5432'
    )

    df_trades = get_dataframe(connection, 'mt4_trades').astype({
        'ticket': 'int',
        'login': 'int',
        'open_time': 'datetime64[ns]',
        'close_time': 'datetime64[ns]',
        'cmd': 'int'
    })

    get_fast_trade_counts_per_user(df_trades)\
        .to_csv('output.csv', index=False)

    connection.close()


if __name__ == "__main__":
    main()
