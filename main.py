import requests
import pandas as pd
from sqlalchemy import create_engine

# Define la URL de la API a consultar
url = "https://api.etherscan.io/api"

#Completamos con los datos de Conexion y Que buscamos
parametros_ultimo_bloque = {
  "module": "proxy",
  "action": "eth_blockNumber",
  "apikey": "3XKGEHIXCRBD58B8K1GY9YRT7KDHG95Y3F"
}

# Realiza la solicitud para obtener el número del último bloque
respuesta = requests.get(url, params=parametros_ultimo_bloque)

# Verifica que la solicitud haya sido exitosa
if respuesta.status_code == 200:
  datos = respuesta.json()

  # Obtiene el número del último bloque
  ultimo_bloque = datos['result']

  parametros_bloque_especifico = {
    "module": "proxy",
    "action": "eth_getBlockByNumber",
    "tag": ultimo_bloque,
    "boolean": "true",
    "apikey":
    "3XKGEHIXCRBD58B8K1GY9YRT7KDHG95Y3F"  # Reemplaza con tu clave API
  }

  # Realiza la solicitud para obtener la información del último bloque
  respuesta_bloque = requests.get(url, params=parametros_bloque_especifico)

  if respuesta_bloque.status_code == 200:
    datos_bloque = respuesta_bloque.json()

    # Procesa los datos JSON a un diccionario de Python
    diccionario = dict(datos_bloque)

    # Intenta convertir los datos en un DataFrame
    try:
      df = pd.DataFrame([diccionario['result']])
      # Transforma el timestamp de segundos desde epoch a formato timestamp
    except ValueError as e:
      print(f"Error al convertir los datos en un DataFrame: {e}")

  else:
    print(f"Error en la petición del bloque: {respuesta_bloque.status_code}")
else:
  print(f"Error en la petición del número de bloque: {respuesta.status_code}")

# Definir las credenciales de la base de datos Redshift

engine = create_engine(
  'redshift+psycopg2://scharfgadiel_coderhouse:JTD9823vUq@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database'
)

# Define la consulta para crear la tabla
create_table_query = """
CREATE TABLE IF NOT EXISTS entregable_1 (
    baseFeePerGas BIGINT,
    difficulty BIGINT,
    extraData VARCHAR(1000),
    gasLimit BIGINT,
    gasUsed BIGINT,
    hash VARCHAR(1000),
    logsBloom VARCHAR(1000),
    miner VARCHAR(1000),
    mixHash VARCHAR(1000),
    nonce VARCHAR(1000),
    number BIGINT,
    parentHash VARCHAR(1000),
    receiptsRoot VARCHAR(1000),
    sha3Uncles VARCHAR(1000),
    size BIGINT,
    stateRoot VARCHAR(1000),
    timestamp BIGINT,
    totalDifficulty BIGINT,
    transactions VARCHAR(1000),
    transactionsRoot VARCHAR(1000),
    uncles VARCHAR(1000),
    withdrawals VARCHAR(1000),
    withdrawalsRoot VARCHAR(1000)
)
'''
connection.execute(create_table_query)

"""

# Ejecuta la consulta para crear la tabla
with engine.connect() as connection:
  connection.execute(create_table_query)

# Usa pandas para escribir los datos del DataFrame a la base de datos , EN PROCESO
df.to_sql('entregable_1', con=engine, if_exists='append', index=False)
