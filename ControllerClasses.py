# Klasse für erleichterte Handhabung der Informationen für den Datenbankzugriff (Engine, Tabellenname, Datentypinformationen, Primärschlüssel, Attribute)
import copy
from sqlalchemy import Engine


class TableMetaData:
    def __init__(self, engine:Engine, table_name:str, primary_keys:list[str], data_type_info:dict[str:dict[str:str]], row_count:int):
        self.engine = engine
        self.table_name = table_name
        self.primary_keys = primary_keys
        self.data_type_info = data_type_info
        self.columns = list(data_type_info.keys())
        self.data_types = []
        for key in data_type_info.keys():
            self.data_types.append(data_type_info[key]['data_type'])
        self.total_row_count = row_count

    def __copy__(self):
        """Erstellen einer Kopie des TableMetaData-Objektes"""
        return copy.copy(self)
    
    def get_data_type(self, column_name:str):
        """Rückgabe des Datentyps des eingegebenen Attributes; None, falls dieses nicht enthalten ist."""
        if column_name in self.data_type_info.keys():
            return self.data_type_info[column_name]['data_type']
        else:
            return None
    
    def get_data_type_group(self, column_name:str):
        """Rückgabe der Datentypgruppe des eingegebenen Attributes; None, falls dieses nicht enthalten ist."""
        if column_name in self.data_type_info.keys():
            return self.data_type_info[column_name]['data_type_group']
        else:
            return None
        
    def get_character_max_length(self, column_name:str):
        """Rückgabe der max. erlaubten Zeichenanzahl des eingegebenen Attributes; None, falls dieses nicht enthalten ist."""
        if column_name in self.data_type_info.keys() and self.data_type_info[column_name]['data_type'] == 'text':
            return self.data_type_info[column_name]['character_max_length']
        else:
            return None
            