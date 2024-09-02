import functions_framework
import pandas as pd
from pandas import json_normalize
import requests
import time
from datetime import datetime, timedelta
import numpy as np
from joblib import load
from google.cloud import storage
from io import BytesIO

headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': "XxXxXxXxXxXxXxXxXxXxXxXx" # Aquí tiene que ir la KEY que hemos obtenido al darnos de alta en https://www.api-football.com/
        }

min_days_available_fixtures = 7
max_days_available_fixtures = 14
wait_for_next_request = False
bucket_name = 'master-uned-tfm'
        
def download_blob(bucket_name, source_blob_name):
    # Crea un cliente de Google Cloud Storage
    storage_client = storage.Client()

    # Accede al bucket
    bucket = storage_client.bucket(bucket_name)

    # Accede al blob (archivo) dentro del bucket
    blob = bucket.blob(source_blob_name)

    # Descarga el contenido del blob como un objeto BytesIO
    model_data = BytesIO(blob.download_as_bytes())

    return model_data
    
def valida_json(json):
    try:
        if json is None:
            return f"No existe json"
        errors = json['errors']
        if (errors != []):
            return f"El json contiene errores: " + str(errors)
        response = json['response']
        if (response == []):
            return f"El json tiene una respuesta vacía"
    except Exception as e:
        return f"Se ha producido una excepción durante la validación del json obtenido: {str(e)}"
    
def request(url):
    global wait_for_next_request
    try:
        if wait_for_next_request:
            today = datetime.today()
            if (today < datetime.strptime("2024-09-10", '%Y-%m-%d')):
                time.sleep(0.25)
            else:
                time.sleep(7)
        else:
            wait_for_next_request = True

        print(f"Nueva petición a la API: {url}")
        response = requests.get("https://v3.football.api-sports.io/"+url, headers=headers)

        if response.status_code == 200:
            json = response.json()
            validacion_json = valida_json(json)
            if not validacion_json is None:
                return f"Error en la petición {url}: {validacion_json}"
            else:
                return json
        else:
            return f"La petición {url} ha devuelto {response.status_code}"
    except Exception as e:
            return f"Se ha producido una excepción durante la petición {url}: {str(e)}"
    
def leer_apuestas_by_day(page=1, day=""):
    return request("odds?date="+day+"&page="+str(page))

def leer_apuestas_by_fixture_by_id(fixture_id):
    return request("odds?fixture="+str(fixture_id))

def leer_fixture_by_id(id):
    return request("fixtures?id="+str(id))
    
def leer_fixtures_by_team_id(team_id):
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    date_from = today.strftime('%Y-%m-%d')
    date_to = (today + timedelta(days=max_days_available_fixtures)).strftime('%Y-%m-%d')
    season = str(today.year)
    return request("fixtures?from="+date_from+"&to="+date_to+"&team="+str(team_id)+"&season="+season)

def leer_fixtures_by_team_id_and_day(team_id, day_str):
    day = datetime.strptime(day_str, '%Y-%m-%d')
    season = str(day.year)
    return request("fixtures?date="+day_str+"&team="+str(team_id)+"&season="+season)
    
def leer_equipo_by_name(name):
    return request("teams?name="+name)
    
def procesar_apuestas(json_odds, df: pd.DataFrame):
    # Con las siguientes líneas obtenemos un dataframe df_odds con información de las apuestas
    bookmakers_data = json_normalize(json_odds['response'],
                        record_path=['bookmakers'],
                        meta=[['fixture', 'id']])
    bookmakers_data.rename(columns={'id':'bookmaker.id','name': 'bookmaker.name'}, inplace=True)
    bets_data = json_normalize(bookmakers_data.to_dict(orient='records'),
                    record_path=['bets'],
                    meta=['fixture.id', 'bookmaker.id', 'bookmaker.name'])
    bets_data.rename(columns={'id':'bet.id','name': 'bet.name'}, inplace=True)
    df_odds = json_normalize(bets_data.to_dict(orient='records'),
                        record_path=['values'],
                        meta=['fixture.id', 'bookmaker.id', 'bookmaker.name', 'bet.id', 'bet.name'])
    # A continuación, vamos a concatenarle la información que tenemos disponible del JSON del evento, conforme a todas las propiedades fixture.id que tenemos.
    # Para ello, formaremos otro df_fixtures y luego lo fusionaremos con nuestro df_odds
    df_fixtures = pd.DataFrame()
    fixtures_ids = df_odds['fixture.id'].unique()
    for fixture_id in fixtures_ids:
        json_fixture = leer_fixture_by_id(fixture_id)
        fixture_data = json_normalize(json_fixture['response'])
        df_fixtures = pd.concat([df_fixtures, fixture_data], ignore_index=True)

    df_fixtures.drop(columns=['events', 'lineups', 'statistics', 'players'], inplace=True) # Eliminamos las propiedades que ya hemos justificado previamente que no vamos a necesitar
    df_merged = pd.merge(df_odds, df_fixtures, on='fixture.id') # Unimos los dataframes de apuestas y eventos utilizando como columna de join fixture.id, de tipo inner (el valor por defecto del parámetro how)

    df = pd.concat([df, df_merged], ignore_index=True)

    return df

def prediccion(df_eval: pd.DataFrame):
    #EDA: Ajuste de los tipos de datos
    df_eval = df_eval.astype({
        'value': 'string',
        'odd': 'float',
        'fixture.id': 'int',
        'bookmaker.id': 'int',
        'bookmaker.name': 'string',
        'bet.id': 'int',
        'bet.name': 'string',
        'fixture.referee': 'string',
        'fixture.timezone': 'string',
        'fixture.timestamp': 'int',
        'fixture.periods.first': 'float',
        'fixture.periods.second': 'float',
        'fixture.venue.id': 'float',
        'fixture.venue.name': 'string',
        'fixture.venue.city': 'string',
        'fixture.status.long': 'string',
        'fixture.status.short': 'string',
        'fixture.status.elapsed': 'float',
        'league.name': 'string',
        'league.country': 'string',
        'league.logo': 'string',
        'league.flag': 'string',
        'league.round': 'string',
        'teams.home.id': 'int',
        'teams.home.name': 'string',
        'teams.home.logo': 'string',
        'teams.home.winner': 'bool',
        'teams.away.id': 'int',
        'teams.away.name': 'string',
        'teams.away.logo': 'string',
        'teams.away.winner': 'bool',
        'goals.home': 'float',
        'goals.away': 'float',
        'score.halftime.home': 'float',
        'score.halftime.away': 'float',
        'score.fulltime.home': 'float',
        'score.fulltime.away': 'float',
        'score.extratime.home': 'float',
        'score.extratime.away': 'float',
        'score.penalty.home': 'float',
        'score.penalty.away': 'float'
    })
    df_eval['fixture.date'] = pd.to_datetime(df_eval['fixture.date'])

    # EDA: Nos quedamos con los tipos de apuestas más populares
    bet_id_to_keep = [10, 5, 31, 4, 6, 7, 26, 19, 16, 17, 9, 25, 62]
    df_eval = df_eval[df_eval['bet.id'].isin(bet_id_to_keep)]

    # Feature Engineering: Creación de la columna bet
    df_eval['bet'] = df_eval['bookmaker.id'].astype(str) + '_' + df_eval['bet.id'].astype(str) + '_' + df_eval['value'].astype(str).str.replace(' ', '')
    df_eval = df_eval.drop_duplicates(subset=['fixture.id', 'bet'])

	  # Feature Engineering: Pivotamos el df_eval
    df_eval = df_eval.pivot(index=['fixture.id', 'fixture.referee', 'fixture.date', 'fixture.venue.id', 'league.id', 'league.round', 'teams.home.id', 'teams.away.id'], columns='bet', values='odd')
    df_eval.reset_index(inplace=True)

    # Tras pivotarlo componemos un df de resultado con el id del partido
    # y luego le añadimos el resultado que hemos predicho
    df_resultados = pd.DataFrame()
    df_resultados['fixture.id'] = df_eval['fixture.id']
    
    # Feature Engineering: Creación de las columnas month, hour
    df_eval['month']=df_eval['fixture.date'].dt.month
    df_eval['hour']=df_eval['fixture.date'].dt.time
    df_eval = df_eval.drop(['fixture.date', 'fixture.id'], axis=1)

    # Feature Engineering: Rellenamos los NAs de las columnas numéricas con 0's
    df_eval = df_eval.fillna({col: 0 for col in df_eval.select_dtypes(include=np.number).columns})

    # Model: One-hot conding: Convertimos las variables object, string y categóricas en booleanos con la función get_dummies
    # NOTA: En esta ocasión tenemos que dejar la primera fila porque si no nos quedamos
    df_eval = pd.get_dummies(df_eval, drop_first=False)

    # Como es más que probable que las columnas no coincidan, utilizamos las que guardamos en el proceso de entranamiento y
    # añadimos las columnas que faltan, elimninamos las que sobran y ajustamos los tipos
    dict_columnas = load(download_blob(bucket_name, 'data/columns.joblib'))
    columnas = list(dict_columnas.keys())
    df_eval_completo = pd.DataFrame(columns=columnas)
    df_eval = df_eval_completo.combine_first(df_eval)
    df_eval = df_eval[columnas]
    # Ajustamos los tipos
    for columna, tipo in dict_columnas.items():
        if tipo == 'int64':
            df_eval[columna] = df_eval[columna].astype('int64')
        elif tipo == 'float64':
            df_eval[columna] = df_eval[columna].astype('float64')
        elif tipo == 'boolean':
            df_eval[columna] = df_eval[columna].astype('boolean')
        elif tipo == 'bool':
            df_eval[columna] = df_eval[columna].astype('bool')
        elif tipo == 'object':
            df_eval[columna] = df_eval[columna].astype('object')
    # Finalmente rellenamos las columnas booleanas con False y las de tipo entero a 0
    columnas_booleanas = df_eval.select_dtypes(include=['boolean','bool']).columns
    df_eval[columnas_booleanas] = df_eval[columnas_booleanas].fillna(False)
    df_eval = df_eval.fillna({col: 0 for col in df_eval.select_dtypes(include=np.number).columns})
    
    # Model: Realizamos la predicción
    modelo = load(download_blob(bucket_name, 'data/model.joblib'))
    resultado = modelo.predict(df_eval)
    df_resultados['prediction'] = resultado
    return df_resultados
    
def es_dia_valido(day_str):
    try:
        day = datetime.strptime(day_str, '%Y-%m-%d')
        today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        max = today + timedelta(days=max_days_available_fixtures)
        min = today - timedelta(days=min_days_available_fixtures)
        if not (min <= day <= max):
            return f"El día proporcionada ({day_str}) debe estar entre {str(min)} y {str(max)}"
    except Exception as e:
        return f"Se ha producido una excepción durante la validación del día {day_str}: {str(e)}"
        
def gestiona_solicitud_equipo(team):
    print(f"Nueva solicitud de predicciones de partidos de fútbol para el equipo {team} entre hoy y los próximos {max_days_available_fixtures} días")
    json_equipo = leer_equipo_by_name(team)
    if isinstance(json_equipo, str):
        return json_equipo
    team_id = json_equipo['response'][0]['team']['id']
    json_fixtures = leer_fixtures_by_team_id(team_id)
    if isinstance(json_fixtures, str):
        return json_fixtures
    fixtures = json_fixtures['response']
    df = pd.DataFrame()
    for json_fixture in fixtures:
        fixture_id = json_fixture['fixture']['id']
        json_odds = leer_apuestas_by_fixture_by_id(fixture_id)
        if isinstance(json_odds, str):
            print(json_odds + ", pero el procesamiento continuará con el resto de partidos")
            continue
        df = procesar_apuestas(json_odds, df)
    return df

def gestiona_solicitud_equipo_y_dia(team, day):
    print(f"Nueva solicitud de predicciones de partidos de fútbol para el día {day} del equipo {team}")
    error_dia = es_dia_valido(day)
    if not error_dia is None:
        return error_dia
    json_equipo = leer_equipo_by_name(team)
    if isinstance(json_equipo, str):
        return json_equipo
    team_id = json_equipo['response'][0]['team']['id']
    json_fixtures = leer_fixtures_by_team_id_and_day(team_id, day)
    if isinstance(json_fixtures, str):
        return json_fixtures
    fixture_id = json_fixtures['response'][0]['fixture']['id']
    json_odds = leer_apuestas_by_fixture_by_id(fixture_id)
    if isinstance(json_odds, str):
        return json_odds
    df = pd.DataFrame()
    df = procesar_apuestas(json_odds, df)
    return df

def gestiona_solicitud_dia(day):
    print(f"Nueva solicitud de predicciones de partidos de fútbol para el día {day}")
    error_dia = es_dia_valido(day)
    if not error_dia is None:
        return error_dia
    json_odds = leer_apuestas_by_day(1, day)
    if isinstance(json_odds, str):
        return json_odds
    pages = json_odds['paging']['total']
    print(f"Número de páginas de apuestas para el día {day}: {pages}")
    df = pd.DataFrame()
    df = procesar_apuestas(json_odds, df)
    for page in range(2, pages + 1):
        json_odds = leer_apuestas_by_day(page, day)
        if isinstance(json_odds, str):
            print(json_odds + ", pero el procesamiento continuará con el resto de apuestas")
            continue
        df = procesar_apuestas(json_odds, df)
    return df


@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    
    request_args = request.args
    if request_args and 'team' in request_args:
        team = request_args['team']
        if 'day' in request_args:
            day = request_args['day']
            df = gestiona_solicitud_equipo_y_dia(team, day)
        else:
            df = gestiona_solicitud_equipo(team)
    else:
        if request_args and 'day' in request_args:
            day = request_args['day']
        else:
            today = datetime.today()
            day = today.strftime('%Y-%m-%d')
        df = gestiona_solicitud_dia(day)
    if isinstance(df, str):
        return df

    print(f"Realizamos la predicción")
    df_resultados = prediccion(df)
    # Fusionamos los df y df_resulado
    df_reducido = df[['fixture.id', 'fixture.date', 'fixture.referee', 'fixture.venue.name', 'fixture.venue.city', 'league.name', 'league.country', 'league.season', 'league.round', 'teams.home.name', 'teams.away.name']].drop_duplicates(subset=['fixture.id'])
    df_final = df_resultados.merge(df_reducido, on='fixture.id', how='left')

    df_final = df_final.sort_values(by='fixture.date') # Ordenamos por fecha
    
    return df_final.to_json(orient='records')