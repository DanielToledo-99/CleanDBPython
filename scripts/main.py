import pyodbc
import pandas as pd
from openpyxl import Workbook
from datetime import datetime
class ExcelToDatabase:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.conn_str = f'DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}'

    def connect_to_database(self):
        try:
            self.conn = pyodbc.connect(self.conn_str)
            print("Conexión exitosa.")
        except Exception as e:
            print("Error al conectar:", e)

    def read_excel_and_process(self, excel_file_path):
        try:
            df = pd.read_excel(excel_file_path)
            df['Extracted'] = df['Unnamed: 3'].str.split('.').str[-1]
            return df.iloc[1:]
        except Exception as e:
            print("Error al leer el archivo Excel:", e)
            return None

    def save_to_database(self, df, table_name):
        if df is not None:
            try:
                cursor = self.conn.cursor()
                id = 1  # Inicializamos id fuera del bucle
                for index, row in df.iterrows():
                    # Aquí deberías ajustar la lógica para insertar los datos en tu tabla de base de datos
                    # Por ejemplo:
                    cursor.execute("""
                    INSERT INTO [PowerLogic].[dbo].[CLASIFICACION_MAST] 
                        ([id], [zona], [tipo_sensor], [etiqueta_visual], [id_nodo], [etiqueta_nodo], [tipo_sennal], [unidad_sennal])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, id, row['Unnamed: 0'], row['Unnamed: 1'], row['Unnamed: 2'], row['Unnamed: 3'], row['Extracted'], row['Unnamed: 4'], row['Unnamed: 5'])
                    id += 1  # Incrementamos id por cada iteración
                self.conn.commit()
                print("Datos guardados en la base de datos.")
            except Exception as e:
                self.conn.rollback()
                print("Error al guardar los datos en la base de datos:", e)

    def close_connection(self):
        try:
            self.conn.close()
            print("Conexión cerrada.")
        except Exception as e:
            print("Error al cerrar la conexión:", e)
    def print_excel_data(self, df, table_name):

        try:
            df = pd.read_excel(excel_file_path)
            print("Datos del archivo Excel:")
            print(df)
        except Exception as e:
            print("Error al leer el archivo Excel:", e)
    def limpieza_datos(self, df, headers):
        try:
            # Crear un nuevo libro de Excel
            wb = Workbook()
            # Activar la hoja de trabajo activa (por defecto)
            ws = wb.active
            ws.append(headers)
            date_columns = [col for col in df.columns if 'date' in col.lower()]
            time_columns = [col for col in df.columns if 'time' in col.lower()]
            
            # Iterar sobre los pares de columnas DATE y TIME
            for date_col, time_col in zip(date_columns, time_columns):
                # Concatenar DATE y TIME para crear DATETIME
                df['DATETIME'] = df[date_col].astype(str) + ' ' + df[time_col].astype(str)
                # Agregar encabezados a la primera fila
                # Agregar los valores de la columna "DATETIME" al archivo Excel
                for value in df['DATETIME']:
                        ws.append([value])
                        
            
            # Guardar el archivo Excel
            new_file_path = ''
            wb.save(new_file_path)
            print(f"Archivo Excel creado satisfactoriamente: {new_file_path}")

        except Exception as e:
            print("Error al crear el archivo Excel:", e)
    def subir_condiciones(self, df):
        try:
            # Filtrar el DataFrame para obtener solo las filas donde la columna 'valor' no sea NaN
            df_filtered = df[df['valor'].notna()]
            
            cursor = self.conn.cursor()
            id = 1
            date_columns = [col for col in df.columns if 'tiempo' in col.lower()]
            time_columns = [col for col in df.columns if 'hora' in col.lower()]
            df_filtered.loc[:, 'DATETIME'] = ''

            # Iterar sobre las columnas de fecha y hora y concatenarlas utilizando .loc[]
            for date_col, time_col in zip(date_columns, time_columns):
                df_filtered.loc[:, 'DATETIME'] = df_filtered[date_col].astype(str) + ' ' + df_filtered[time_col].astype(str) + ' '
            print(df_filtered)
            # Iterar sobre las filas del DataFrame filtrado y ejecutar una consulta INSERT INTO para cada una
            for index, row in df_filtered.iterrows():
                valor = float(row['valor'])
                cluster = "S_H15_X"
                fecha = row['DATETIME']
                cursor.execute("""
                    INSERT INTO [PowerLogic].[dbo].[PCL_SECADEROS]
                        ([id], [sensor], [valor], [fechaHora])
                        VALUES (?, ?, ?, ?)
                    """, (id, cluster, valor, fecha))
                
                id += 1
            
            self.conn.commit()
            print("Datos guardados en la base de datos.")
        
        except Exception as e:
            self.conn.rollback()
            print("Error al guardar los datos en la base de datos:", e)




# Uso de la clase
server = 
database = 
username = 
password = 

# Uso de la clase
excel_file_path = '../scripts/nuevo_excel.xlsx' 

excel_to_db = ExcelToDatabase(server, database, username, password)

# Conectar a la base de datos
excel_to_db.connect_to_database()

df = pd.read_excel(excel_file_path)
# excel_to_db.print_excel_data(df,table_name)

excel_to_db.subir_condiciones(df)

# Cerrar la conexión a la base de datos
excel_to_db.close_connection()cls