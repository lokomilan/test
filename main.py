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


def get_fast_trade_counts_by_user(df, limit_seconds=60):
    return df\
        .assign(
            duration=(df.close_time - df.open_time).dt.total_seconds(),
            is_fast_trade=lambda row: row.duration.between(0, 60, inclusive='left'))\
        .groupby('login', as_index=False)\
        .agg({'is_fast_trade': 'sum'})\
        .rename(columns={'is_fast_trade': 'fast_trades_count'})\
        .sort_values('login')


def get_paired_order_counts_by_user(df, limit_seconds=30):
    df_sorted = df\
        .assign(
            is_sale=df.cmd,
            is_purchase=(1 - df.cmd))\
        .sort_values(['login', 'open_time'])\
        .reset_index(drop=True)
    df_last_30s = df_sorted\
        .groupby('login', as_index=False)\
        .rolling(window='{}s'.format(limit_seconds), on='open_time', closed='left')\
        .agg({
            'is_sale': 'sum',
            'is_purchase': 'sum'
        })\
        .fillna(0)\
        .reset_index(drop=True)\
        .rename(columns={
            'is_sale': 'user_sales_count_window',
            'is_purchase': 'user_purchases_count_window'
        })\
        .astype({
            'user_sales_count_window': 'int',
            'user_purchases_count_window': 'int',
        })
    df_full = pd.concat([df_sorted, df_last_30s], axis=1)
    df_full['paired_orders_count'] = df_full.apply(
        lambda row: row.user_purchases_count_window
        if row.is_sale == 1
        else row.user_sales_count_window,
        axis=1,
    )
    return df_full\
        .groupby('login', as_index=False)\
        .agg({'paired_orders_count': 'sum'})\
        .sort_values('login')


def get_user_pairs_with_connected_orders(df, orders_threshold=10):
    df['open_time_30s'] = df.open_time.dt.floor('30s')
    return df\
        .merge(df, on=['open_time_30s', 'symbol'])\
        .query('login_x < login_y and cmd_x + cmd_y == 1')\
        .groupby(['login_x', 'login_y'], as_index=False)\
        .agg({'ticket_x': 'count'})\
        .query('ticket_x > {}'.format(orders_threshold))\
        .sort_values(['login_x', 'login_y'])\
        .get(['login_x', 'login_y'])


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

    df_marked_trades = get_dataframe(connection, 'mt4_marked_trades').astype({
        'positionid': 'int',
        'type': 'int'
    })
    df_blacklisted_trades = df_marked_trades[df_marked_trades['type'] & 2 > 0]

    df_trades = pd.merge(
        df_trades,
        df_blacklisted_trades,
        how='left',
        left_on='ticket',
        right_on='positionid',
        indicator=True
    ).query('_merge == "left_only"')

    pd.merge(
        get_fast_trade_counts_by_user(df_trades),
        get_paired_order_counts_by_user(df_trades),
        on='login'
    ).sort_values('login')\
        .to_csv('metrics_by_login.csv', index=False)

    get_user_pairs_with_connected_orders(df_trades)\
        .to_csv('user_pairs.csv', index=False)

    connection.close()


if __name__ == '__main__':
    main()
