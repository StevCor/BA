from sqlalchemy import Engine

from model.databaseModel import get_column_names_data_types_and_max_length, get_primary_key_from_engine


class TableMetaData:
    def __init__(self, engine:Engine, table_name:str, row_count:int):
        self.engine = engine
        self.table = table_name
        self.primary_keys = get_primary_key_from_engine(engine, table_name)
        column_names_and_data_types = get_column_names_data_types_and_max_length(engine, table_name)
        self.columns = list(column_names_and_data_types.keys())
        self.data_types = []
        for key in column_names_and_data_types.keys():
            self.data_types.append(column_names_and_data_types[key]['data_type'])

        self.column_names_and_data_types = column_names_and_data_types
        self.total_row_count = row_count