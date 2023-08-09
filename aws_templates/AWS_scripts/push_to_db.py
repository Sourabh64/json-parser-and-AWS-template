import psycopg2
import sqlalchemy
from json_parser import Parser


class DB:
    def __init__(self):
        self.host =
        self.username =
        self.password =
        self.port =
        self.db =

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

    def get_dtype_conversions(self, df):
        dtype_conversion_dict = {'int64': 'int8', 'object': 'varchar', 'float64': 'float8', 'datetime64[ns]': 'timestamp',
                                 'datetime64': 'timestamp', 'datetime64[ns, tzlocal()]': 'timestamp', 'bool': 'boolean',
                                 'datetime64[ns, UTC]': 'timestamp', 'datetime64[ns, tzutc()]': 'timestamp'}
        rs_dtype_list = []
        new_one = (df.applymap(type) == list).all()
        list_type = new_one.index[new_one].tolist()
        for i, v in zip(df.dtypes.index, df.dtypes.values):
            rs_dtype = dtype_conversion_dict[str(v)]
            if v == "object":
                try:
                    if df[i].str.len().max() < 1:
                        if i in list_type:
                            rs_dtype += ' array'
                        else:
                            rs_dtype += '({})'.format(int(25))
                    else:
                        if i in list_type:
                            rs_dtype += ' array'
                        else:
                            if df[i].str.len().max() == "nan":
                                print(df[i].str.len().max())
                            rs_dtype += '({})'.format(int(df[i].str.len().max()))
                except AttributeError:
                    print("Attribute Error for {}, {}".format(i, v))
            rs_dtype_list.append(rs_dtype)
        return rs_dtype_list

    def create_query(self, df, schema, table):
        cols = df.dtypes.index
        rs_dtype_list = self.get_dtype_conversions(df)
        vars = ', '.join(['{0} {1}'.format(str(x), str(y)) for x, y in zip(cols, rs_dtype_list)])
        query = '''create table IF NOT EXISTS {}.{} ({});'''.format(schema, table, vars)
        return query

    def db_process(self, df_dict):
        print("Connecting to DB")
        conn, cursor = self.connect()
        try:
            for df in df_dict:
                df_dict[df].columns = df_dict[df].columns.str.lower()
                query = self.create_query(df_dict[df], 'aws', df)
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
                    new_table_query = query
                    self.execute_query(cursor, new_table_query)
                    conn.commit()
                engine = sqlalchemy.create_engine()
                df_dict[df].to_sql(table_name, con=engine, schema='aws', if_exists='append', index=False)
            return True
        except Exception as e:
            print(e)
