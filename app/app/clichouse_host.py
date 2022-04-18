from os import environ

CLICKHOUSE_HOST = environ.get('CLICKHOUSE_HOST', 'clickhouse')
