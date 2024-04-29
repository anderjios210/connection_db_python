import sqlalchemy 
from sqlalchemy import create_engine
import pyodbc 
import pandas as pd

def connection(server, db):
    try:
        # Construir la cadena de conexión
        engine = create_engine(f"mssql+pyodbc://@{server}/{db}?driver=ODBC+Driver+17+for+SQL+Server")
        
        # Realizar la conexión
        conexion = engine.connect()
        
        print("Conexión exitosa a la base de datos")
        return conexion
    
    except Exception as e:
        print("Error al conectar a la base de datos:", e)
        return None    
    
def ejecutar_query(consulta):
    try:
        # palabras prohibidas
        palabras_prohibidas = ['DELETE', 'DROP', 'ALTER TABLE']        
        if any(keyword in consulta.upper() for keyword in palabras_prohibidas):
            print("No se permiten consultas DELETE, DROP o ALTER TABLE.")
            return
        
        # Ejecutar la consulta
        conn = connection("Anderson\\SQLEXPRESS", "Spanish")
         # Ejecutar la consulta y guardar los resultados en un DataFrame
        df_resultado = pd.read_sql_query(consulta, conn)
        
        print("Consulta ejecutada correctamente:", consulta)
        return df_resultado
    
    except Exception as e:
        print("Error al ejecutar la consulta:", e)

# Ejemplo de uso de la función
consulta = "SELECT TOP(2) * FROM dbo.DimProduct;"  # Ejemplo de consulta SQL

# Llamar a la función ejecutar_query con la consulta
ejecutar_query(consulta)