import glob
from tqdm import tqdm
import pandas as pd
import os
import yaml
import time
import traceback
import psycopg2
from psycopg2 import Error
import psycopg2.sql as sql

"""
DB operations:
    1. Update the common columns
    2. Add new columns where necessary
    3. Delete the rows which do not exist in the new sheet
"""


class UpdateDB():

    def __init__(self, df_path):
        # Create a connection to postgres DB
        self.connection = self.connect_to_db()
        # Create a cursor to perform database operations
        self.cursor = self.connection.cursor()
        self.df_path = df_path
        self.tbl_name = self.get_tbl_name()


    def connect_to_db(self):
        try:
            data = self.read_config_yaml()
            connection = psycopg2.connect(user=data['user'],
                                          password=data['password'],
                                          host=data['host'],
                                          port=data['port'],
                                          database=data['database'])

            print("PostgreSQL server connection opened\n")
            return connection

        except Exception as error:
            print("Error while connecting to PostgreSQL", error)

    def read_config_yaml(self):
        with open("config.yaml", "r") as yamlfile:
            cfg = yaml.load(yamlfile, Loader=yaml.FullLoader)
        return cfg['database_details']

    def get_tbl_name(self):
        tbl_name = "preorder_" + os.path.splitext(os.path.basename(self.df_path))[0]
        return tbl_name

    def get_df_from_csv(self):
        df = pd.read_csv(self.df_path)  # dataframe of the csv
        df = df.astype(str)
        return df

    def get_df_from_db(self):
        select_query = f"SELECT * FROM {self.tbl_name}"
        self.cursor.execute(select_query)
        rec = self.cursor.fetchall()
        column_names = [desc[0] for desc in self.cursor.description]
        df_tbl = pd.DataFrame(rec, columns=column_names)
        return df_tbl

    def compare_csv_and_db_columns(self):
        df_csv = self.get_df_from_csv()
        df_db = self.get_df_from_db()
        print(df_csv.columns)
        print(df_db.columns)
        print(f'are columns of both the dfs same {df_csv.columns == df_db.columns}')

    """
        Using this script we are inserting the data for the first time in DB, hence we are going to delete the 
        old tables and create them again and insert the data parsed from this script.
        """

    # delete the old tables

    def delete_old_table(self):
        query = f"DROP TABLE {self.tbl_name}"
        try:
            self.cursor.execute(query)
            print(f'Table {self.tbl_name} dropped successfully')

        except (Exception, psycopg2.Error) as error:
            print(f'Cannot delete table {self.tbl_name}', error)
            self.connection.rollback()
            traceback.print_exc()

    # to insert data, we need to have a tuple that states column names and their types

    def set_column_with_their_types(self, column):
        if column == "serial_key":
            db_column = (column, 'character varying PRIMARY KEY')
        if column != "serial_key":
            db_column = (column, 'character varying')
        return db_column

    def get_columns_with_their_types(self, new_df):
        columns_with_their_types = []
        for column in list(new_df.columns):
            column_ = self.set_column_with_their_types(column)
            columns_with_their_types.append(column_)
        return tuple(columns_with_their_types)

    def query_to_create_table_in_db(self, columns_with_their_types):
        fields = []
        for col in columns_with_their_types:
            fields.append(sql.SQL("{} {}").format(sql.Identifier(col[0]), sql.SQL(col[1])))
        query = sql.SQL("CREATE TABLE {tbl_name} ( {fields} );").format(
            tbl_name=sql.Identifier(self.tbl_name),
            fields=sql.SQL(', ').join(fields)
        )
        return query

    def create_table_of_df(self, columns_with_their_types):
        query = self.query_to_create_table_in_db(columns_with_their_types)
        try:
            self.cursor.execute(query)
            self.connection.commit()
            print(f'Table {self.tbl_name} created successfully!')

        except (Exception, psycopg2.Error) as error:
            print('Cannot create table', error)
            self.connection.rollback()
            traceback.print_exc()

    def insert_data_of_df(self, table_df):
        try:
            columns = []
            # Comma-separated string of column names
            columns = ','.join([f'"{col}"' for col in list(table_df.columns)])

            # Comma-separated string of parameter placeholders
            param_placeholders = ','.join(['%s' for val in range(table_df.shape[1])])

            # INSERT command, including the two items above
            sql_query = f"INSERT INTO {self.tbl_name} ({columns}) VALUES ({param_placeholders})"

            for row in table_df.values:
                # tuple of parameter values
                param_values = tuple(value for value in row)
                # execute the command
                self.cursor.execute(sql_query, param_values)

            print(f'Data inserted in table {self.tbl_name} successfully!')

        except (Exception, psycopg2.Error) as error:
            print(f'Cannot insert data in table {self.tbl_name}. Error:', error)
            self.connection.rollback()
            traceback.print_exc()

        self.connection.commit()
        self.connection.close()
        print("PostgreSQL server connection closed\n")

    # db operations
    def do_db_operation(self):
        #self.compare_csv_and_db_columns()
        #  delete table, this is for one-time only
        self.delete_old_table()
        # df to insert into db
        df = self.get_df_from_csv()
        # df columns to be inserted
        df_columns_with_their_types = self.get_columns_with_their_types(df)
        # create new table in db
        self.create_table_of_df(df_columns_with_their_types)
        # insert data of df in db
        self.insert_data_of_df(df)




start_time = time.time()

for csv in tqdm(glob.glob('./csvs_generated/*.csv')):
    print(csv)
    try:
        update_db_obj = UpdateDB(csv)
        update_db_obj.do_db_operation()
    except Exception as error:
        print("Error", error)
print(f'Execution time {time.time() - start_time} seconds')

