import copy
from sqlalchemy import Engine


class TableMetaData:
    def __init__(self, engine:Engine, table_name:str, primary_keys:list[str], data_type_info:dict[str:dict[str:str]], column_names_and_data_types:dict, row_count:int):
        self.engine = engine
        self.table_name = table_name
        self.primary_keys = primary_keys
        self.data_type_info = data_type_info
        column_names_and_data_types = column_names_and_data_types
        self.columns = list(column_names_and_data_types.keys())
        self.data_types = []
        for key in column_names_and_data_types.keys():
            self.data_types.append(column_names_and_data_types[key]['data_type'])

        self.column_names_and_data_types = column_names_and_data_types
        self.total_row_count = row_count

    def __copy__(self):
        return copy.copy(self)
    
    def get_data_type(self, column_name:str):
        if column_name in self.data_type_info.keys():
            return self.data_type_info[column_name]['data_type']
        else:
            return None
    
    def get_data_type_group(self, column_name:str):
        if column_name in self.data_type_info.keys():
            return self.data_type_info[column_name]['data_type_group']
        else:
            return None
        
    def get_character_max_length(self, column_name:str):
        if column_name in self.data_type_info.keys() and self.data_type_info[column_name]['data_type'] == 'text':
            return self.data_type_info[column_name]['character_max_length']
        else:
            return None
            