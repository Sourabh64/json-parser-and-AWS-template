import pandas as pd
from datetime import date


class Parser:
    def __init__(self):
        self.col_list = []
        self.df_dict = {}

    def normalize_json(self, data_list):
        df = pd.json_normalize(data_list)
        for col in df.columns:
            if col not in self.col_list:
                self.col_list.append(col)
        return df

    def get_dtype_conversions(self, df):
        dtype_conversion_dict = {'int64': 'int8', 'object': 'varchar', 'float64': 'float8', 'datetime64[ns]': 'timestamp',
                                 'datetime64': 'timestamp', 'datetime64[ns, tzlocal()]': 'timestamp', 'bool': 'boolean',
                                 'datetime64[ns, UTC]': 'timestamp'}
        rs_dtype_list = []
        for i, v in zip(df.dtypes.index, df.dtypes.values):
            rs_dtype = dtype_conversion_dict[str(v)]
            if v == "object":
                try:
                    if df[i].str.len().max() < 1:
                        # To avoid varchar(e)
                        rs_dtype += '({})'.format(int(1))
                    else:
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

    def data_frame_append(self, df_data, path, old_path=None, key=None):
        df = df_data
        if path in self.df_dict:
            self.df_dict['{}'.format(path)] = pd.concat([self.df_dict['{}'.format(path)], df], axis=0)
            self.df_dict['{}'.format(path)]['current_key'] = range(1, 1+len(self.df_dict['{}'.format(path)]))
            if old_path:
                if key in self.df_dict['{}'.format(old_path)].columns:
                    self.df_dict['{}'.format(old_path)].drop(key, axis=1, inplace=True)
            else:
                pass
        else:
            self.df_dict['{}'.format(path)] = df_data
            self.df_dict['{}'.format(path)]['current_key'] = self.df_dict['{}'.format(path)].index+1

    def process_data(self, data, path):
        parent_key = 0
        meta_dict = {}
        for each in data:
            parent_key += 1

            def flatten(new_data, path=path, parent_key=None):
                if path not in meta_dict:
                    meta_dict[path] = {
                        'current_key': 0,
                        'parent_key': parent_key
                    }
                meta_dict[path]['parent_key'] = parent_key
                if isinstance(new_data, dict):
                    meta_dict[path]['current_key'] += 1
                    for key in new_data:
                        if isinstance(new_data[key], list) and new_data[key]:
                            internal_df = self.normalize_json(new_data[key])
                            internal_df['parent_key'] = parent_key
                            self.data_frame_append(internal_df, path+"_"+key, path, key)
                            flatten(new_data[key], path+"_"+key, parent_key)
                elif isinstance(new_data, list) and new_data:
                    for key in new_data:
                        if isinstance(key, dict):
                            if meta_dict[path]['current_key'] == 0:
                                parent_key = meta_dict[path]['current_key'] + 1
                                meta_dict[path]['current_key'] += 1
                            else:
                                parent_key = meta_dict[path]['current_key']
                            flatten(key, path, parent_key)
                        # elif isinstance(key, str):
                        #     column_name = path.split("_")[-1]
                        #     new_df = pd.DataFrame(new_data, columns=[f'{column_name}'])
                        #     data_frame_append(new_df, path)

            flatten(each, parent_key=parent_key)

    def process(self, data, path):
        data_frame = self.normalize_json(data)
        data_frame['parent_key'] = range(1, 1+len(data_frame))
        self.data_frame_append(data_frame, path)
        self.process_data(data, path)
        today = date.today()
        for i in self.df_dict:
            self.df_dict[i]['date'] = pd.to_datetime(today)
            query = self.create_query(self.df_dict[i], 'AWS', i)
            self.df_dict[i]['query'] = query
        return self.df_dict
