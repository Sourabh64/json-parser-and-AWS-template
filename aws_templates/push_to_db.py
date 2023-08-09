import psycopg2
from json_parser import Parser


class DB:
    def __init__(self):
        self.host = "cmdb.cluster-cwlt8slqy9sh.ap-south-1.rds.amazonaws.com"
        self.username = "cmdb_app"
        self.password = "ZrV97VeHv2"
        self.port = 5432
        self.db = "cmdb"

    def connect(self):
        connection = psycopg2.connect(host=self.host, user=self.username, password=self.password, port=self.port,
                                      database=self.db)
        cursor = connection.cursor()
        return connection, cursor

    def execute_query(self, cur, query):
        try:
            cur.execute(query)
            return cur.fetchall()
        except Exception as e:
            print(e)

    def db_process(self, df_dict):
        conn, cursor = self.connect()
        try:
            for df in df_dict:
                df_cols = list(df_dict[df].columns.str.lower().values)
                table_cols = []
                table_name = df.lower()
                table_exist_query = "SELECT table_name from information_schema.tables where table_schema = 'aws' and table_name = '{}'".format(table_name)
                query_response = self.execute_query(cursor, table_exist_query)
                if table_name in query_response:
                    columns_query = "select column_name from information_schema.columns where table_name = '{}'".format(table_name)
                    table_cols.extend(columns_query)
                    new_cols = set(df_cols) - set(table_cols)
                    if new_cols:
                        parser = Parser()
                        cols = df_dict[df].dtypes.index
                        data_type_list = parser.get_dtype_conversions(df_dict[df])
                        for col in new_cols:
                            data_type = "NULL"
                            for x, y in zip(cols, data_type_list):
                                if x == col:
                                    data_type = str(x) + " " + str(y)
                            column_add_query = "ALTER TABLE aws.{} ADD {} {};".format(table_name, col, data_type)
                            self.execute_query(cursor, column_add_query)
                    else:
                        new_table_query = df_dict[df]['query']
                        self.execute_query(cursor, new_table_query)
                df_dict[df].to_sql(table_name, conn, if_exists='append')
            return True
        except Exception as e:
            print(e)
