## Este programa es para:
    ## Hacer conexión a UNIGIS.
    ## Hace petición de información en la página dashboards.
    ## Genera y formatea un data frame con dicha información para
        ## Enviar finalmente la informacion a una DB.

#Importamos las librerias necesarias para hacer una conexion con la bd de postgres y pandas
import os
import time
import json
import pytz
import psycopg2
import requests
import datetime
import numpy as np
import pandas as pd
from io import BytesIO

###########################################################################################################
#                                                Funciones                                                #
###########################################################################################################

def login_unigis(user="",pas = ""):

    import requests
    import os
    import pandas as pd

    """
    Función que realiza el login en la plataforma de Unigis y devuelve la sesión y el token csrf listos para hacer peticiones
    de exportación de reportes de la sección de dashboard.
    """


    #Creamos los objetos de sesión y cookies
    sessio = requests.Session()
    jar = requests.cookies.RequestsCookieJar()

    url =  "https://heineken2.unigis.com/HEINEKEN/Login.aspx"

    #Creamos la petición
    #Peticion para obtener las cookies y csrf_token de seguimiento
    response = sessio.get(url)
    jar = response.cookies

    #Obtenemos el token csrf
    csrf_token = response.text
    csrf_token = csrf_token.split('name="csrf_token" value="')
    csrf_token = csrf_token[1].split('" />')[0]

    #Creamos el header y el body de la petición de login
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    body = {
        "action" : "login",
        "username" : user,
        "password" : pas,
        "csrf_token" : csrf_token
    }

    #Hacemos la petición de login
    response = sessio.post(url, headers=headers, data=body, cookies=jar)

    ####################################################################
    #Realizamos a la pagina de dashboard
    url_dashboard = "https://heineken2.unigis.com/HEINEKEN/Apps/Dashboard/Default.aspx?source=dashboard"

    response = sessio.get(url_dashboard)
    #Obtenemos el token csrf
    csrf_token = response.text
    csrf_token = csrf_token.split('csrf_token" value="')
    csrf_token = csrf_token[1].split('" />')[0]

    return sessio,jar,csrf_token


###########################################################################################################
#                                          Descarga de los datos                                          #
###########################################################################################################

def lambda_handler(event, context):
    
    
    user_unigis = os.getenv("user_unigis")


    if user_unigis is None:
        with open('user_unigis.json') as file_params:
            user_unigis = json.load(file_params)
        file_params.close()
    else:
        user_unigis = json.loads(user_unigis)

    pas = user_unigis['password']
    usr = user_unigis['user']

    Main_path = os.getcwd()
    PATH_Download = os.path.join(Main_path,"Alertas" )

    # Esta es una implementación que había hecho jorge paraotro tablero.
    # # Guardamos en una variable la fecha del lunes anterior
    # Recordar ajutar a la zona horaria de cada DR.
    fecha_now = datetime.datetime.now(datetime.timezone.utc)
    fecha = datetime.datetime.now(datetime.timezone.utc)
    fecha_now = fecha_now.strftime("%d/%m/%Y")


    # Definir la zona horaria de México
    zona_horaria_mexico = pytz.timezone('America/Mexico_City')

    #Hacemos el login en la plataforma de Unigis
    sessio,jar,csrf_token = login_unigis(user=usr,pas=pas)

    #Hacemos la petición de los datos
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    with open("Parametros.txt","r") as file_param:
        parametros = file_param.read()
    file_param.close()

    body = {
        "ID_REPORTE":"117",
        "PARAMETROS":parametros.format(fecha_now,fecha_now,fecha_now,fecha_now,fecha_now,fecha_now,fecha_now),
        "csrf_token":csrf_token
    }
    del csrf_token
    del parametros
    del fecha_now

    url_datos = "https://heineken2.unigis.com/HEINEKEN/Apps/Dashboard/ExportadorCSV.aspx"
    #(6s)
    response = sessio.post(url_datos, headers=headers, data=body, cookies=jar)
    del sessio
    del jar
    del headers
    del body



    print(response.status_code)
    if response.status_code != 200:

        print("Error en la descarga de los datos")
        return {
            'statusCode': 500,
            'body': json.dumps('Error en la descarga de los datos desde unigis')
            , 'content' : response.text
        }

    csv = BytesIO(response.content)                                                                        # Variable de paso dónde se pone la información antes de terminar el proceso de guardado
    del response

    
    try:
        # Cargar el archivo csv desde la respuesta (7s)
        df_response = pd.read_csv(csv, sep=';', on_bad_lines = 'skip')
        #Remplazamos los NaN por None
        df_response = df_response.where(pd.notnull(df_response), None)
    except:
        print("No es un csv")
        return {
                    'statusCode': 500,
                    'body': json.dumps('No se descargo un csv'), 
                    'content' : csv
                }

    #Transformamos el dataframe a un json
    # df_transform = df_transform.to_json(orient="table")   
    columns = df_response.columns.tolist()
    values = df_response.values.tolist()
    
    df_response = {
        "Columnas":columns,
        "Datos":values
    }
    del columns
    del values
    
    df_response = json.dumps(df_response,ensure_ascii=False)
    
    
    return {
        'statusCode': 200,
        'body': df_response
        # 'query' : f"SELECT fecha_alarma, referencia_externa, codigo, tipo_alerta, id_viaje FROM public.alarmas_diario WHERE fecha_alarma >= '{fecha_inicio_str} 00:00:00' AND fecha_alarma <= '{fecha_str} 23:59:59'"
    }