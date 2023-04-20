'''
Creates a schema and the tables for Fighting Churn With Data in a PostgreSQL database.

The schema name to create is specified by the constant `schema_name` - change that to
create a different schema.

The database and login information are in environment variables:
CHURN_DB
CHURN_DB_USER
CHURN_DB_PASS

The SQL statements to create the tables are the adjacent directory ../schema

'''

from postgres import Postgres
import os
import sys


def drop_test_schema():
    con_string = f"postgresql://{os.environ.get('CHURN_DB_HOST','localhost')}/{os.environ['CHURN_DB']}?user={os.environ['CHURN_DB_USER']}&password={os.environ['CHURN_DB_PASS']}"
    db = Postgres(con_string)
    db.run('DROP SCHEMA IF EXISTS test CASCADE;')


def setup_churn_db(schema_name):

    con_string = f"postgresql://{os.environ.get('CHURN_DB_HOST','localhost')}/{os.environ['CHURN_DB']}?user={os.environ['CHURN_DB_USER']}&password={os.environ['CHURN_DB_PASS']}"
    db = Postgres(con_string)

    tables = ['event', 'subscription', 'event_type', 'metric', 'metric_name',
              'active_period', 'observation', 'active_week', 'account']

    print('Creating schema %s (if not exists)...' % schema_name)
    db.run('CREATE SCHEMA IF NOT EXISTS %s;' % schema_name)

    for t in tables:
        file_root = os.path.abspath(os.path.dirname(__file__))
        with open('%s/schema/create_%s.sql' % (file_root, t), 'r') as sqlfile:
            sql = sqlfile.read().replace('\n', ' ')
        sql = sql.replace('x.', '%s.' % schema_name)
        print('Creating table %s (if not exists)' % t)
        db.run(sql)


if __name__ == "__main__":
    schema_name = 'socialnet7'
    if len(sys.argv) >= 2:
        schema_name = sys.argv[1]
    setup_churn_db(schema_name)
