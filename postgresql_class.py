import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import URL
import sys, io

class PSQL:
    def __init__(self, user, password, host='localhost', database='postgres'):
        """
        initializes class object
        :param user: database username
        :param password: database usernames password
        :param host: defaults to postgres default 'localhost' server
        :param database: defaults to postgres default 'postgres' database
        """
        self.params_dict = {
            'host': host,
            'database': database,
            'user': user,
            'password': password
        }
        self.url_object = URL.create(
            'postgresql+psycopg2',
            username=self.params_dict['user'],
            password=self.params_dict['password'],
            host=self.params_dict['host'],
            database=self.params_dict['database']
        )
        try:
            self.engine = create_engine(self.url_object)
        except Exception as e:
            print('error: ', e)
            print('exception type: ', type(e))
            sys.exit(1)
        print('connection successful')
        self.insp = inspect(self.engine)

    def create_new_db(self, dbname):
        """
        utilizes current DB engine in order to create a new database within the same server
        :param dbname: string representing database to be created
        :return:
        """
        create_db_query = 'CREATE DATABASE '+dbname+';'
        with self.engine.begin() as connection:
            try:
                connection.execute(text('commit'))
                connection.execute(text(create_db_query))
                print(dbname, 'database created!')
            except Exception as e:
                print('error: ', e)
                print('exception type: ', type(e))
                sys.exit(1)

    def create_schemas(self, schemas):
        """
        takes list of schemas as input and creates a new schema in DB for each item
        :param schemas: list of schemas to create
        :return: no return
        """
        with self.engine.begin() as connection:
            try:
                for schema in schemas:
                    connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema.lower()};'))
                    print(f'{schema} schema created!')
            except Exception as e:
                print('error: ', e)
                print('exception type: ', type(e))
                sys.exit(1)

    def create_tbl(self, dataframe, tbl_name, schema='public', if_exists='fail'):
        """
        takes dataframe information as input and creates an empty PSQL table based on the data
        :param dataframe: dataframe to be used to create skeleton tbl
        :param tbl_name: name of table to be creates
        :param schema: schema to add table to
        :param if_exists: what to do if table already exists. Default is to 'fail' to prevent data deletion
        :return: no return
        """
        try:
            # takes column names and data types and initializes empty table in DB
            dataframe.head(0).to_sql(name=tbl_name.lower(), con=self.engine, schema=schema, if_exists=if_exists, index=False)
            print(tbl_name, 'table created!')
        except Exception as e:
            print('error: ', e)
            print('exception type: ', type(e))
            sys.exit(1)

    def insert_into_tbl(self, dataframe, tbl_name, schema='public', filename=None):
        """
        uses psycopg2 method copy_expert in order to copy data from either an in-memory buffer
        or csv file. Much faster than pd.to_sql(). If filename is specified, df will be pushed to file
        and then copied into sql table.
        :param dataframe: pandas df containing data to insert
        :param tbl_name: string containing name of table
        :param schema: string containing name of schema
        :param filename: string containing filename if user wants the data saved to csv.
                Defaults to using an in-memory buffer
        :return: no return
        """
        if filename:
            # With user given filename
            dataframe.to_csv(filename, header=False, index=False)
            stdin = open(filename, 'r', encoding='utf8')
        else:
            # With in-memory buffer
            stdin = io.StringIO()
            dataframe.to_csv(stdin, header=False, index=False)
            stdin.seek(0)
        # Using psycopg2 connection so that copy_expert method can be utilized for fast inserts
        con = psycopg2.connect(**self.params_dict)
        cursor = con.cursor()
        copy_sql = 'COPY ' + schema + '.' + tbl_name + """ FROM stdin WITH CSV HEADER"""
        try:
            cursor.copy_expert(sql=copy_sql, file=stdin)
            con.commit()
            con.close()
            print('Data inserted into', tbl_name)
        except Exception as e:
            con.close()
            print('error: ', e)
            print('exception type: ', type(e))
            sys.exit(1)

    def bulk_insert_into_tbl_from_csv(self, filename, tbl_name, schema='public'):
        con = psycopg2.connect(**self.params_dict)
        cursor = con.cursor()
        stdin = open(filename, 'r', encoding='utf8')
        copy_sql = 'COPY ' + schema + '.' + tbl_name + """ FROM stdin WITH CSV HEADER"""
        try:
            cursor.copy_expert(sql=copy_sql, file=stdin)
            con.commit()
            con.close()
            print('Data inserted into', tbl_name)
        except Exception as e:
            con.close()
            print('error: ', e)
            print('exception type: ', type(e))
            sys.exit(1)

    def get_schemas(self) -> list:
        """
        returns list of schemas within DB
        :return: list of schemas
        """
        try:
            schemas = self.insp.get_schema_names()
            return schemas
        except Exception as e:
            print('error: ', e)
            print('exception type: ', type(e))
            sys.exit(1)

    def get_database_layout(self, schemas=None) -> dict:
        """
        returns DB layout including schemas and tables within the schemas. Can also take list of schemas
        as input in order to exclude some
        :param schemas: takes list of schemas to return tables for - defaults to result of schema query
        :return: dictionary containing dictionary for each schema that contains a list of tables
        """
        layout = {}
        try:
            if not schemas:
                schemas = self.insp.get_schema_names()
            for schema in schemas:
                tables = self.insp.get_table_names(schema)
                layout[schema] = tables
            return layout
        except Exception as e:
            print('error: ', e)
            print('exception type: ', type(e))
            sys.exit(1)

    def query_db(self, query) -> pd.DataFrame:
        with self.engine.begin() as connection:
            try:
                df = pd.read_sql(sql=text(query), con=connection)
                print('query successful')
                return df
            except Exception as e:
                print('error: ', e)
                print('exception type: ', type(e))
                sys.exit(1)
