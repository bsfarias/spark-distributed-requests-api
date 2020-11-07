import json
import sys
import urllib
import requests
from pyspark.sql import SparkSession
from datetime import datetime  
from datetime import timedelta

def get_data_asteroid(params):
    """
    Retorna um payload com os dados de asteroides disponibilizados pela api da nasa
            Parametros:
                    params (List) : parametros da request a ser enviada para a api
            Retorno:
                    payload (obj): json.
    """
    encoded = urllib.parse.urlencode(params)
    response = requests.get("https://api.nasa.gov/neo/rest/v1/feed",params=encoded)
    payload  = json.dumps(response.json())
    return payload


def transform_data(spark_session, rdd):
    """
    Transforma um rdd em um dataframe
            Parametros:
                    spark_session (obj): Sessão spark.
                    rdd (obj): rdd com os payloads retornados pela função get_data_asteroid.
            Retorno:
                    df (obj): DataFrame.
    """
    spark_session.read.json(rdd).createOrReplaceTempView("tmp")
    df = spark_session.sql("""SELECT element_count,near_earth_objects.* FROM tmp""")
    return df

def generate_request_params(api_key, start_date, end_date):
    """
    Gera os parâmetros da requisição dinamicamente de acordo com o range de data informado.
            Parametros:
                        api_key (str)         : Chave utilizada nas requisições para a api
                        start_date (datetime) : Data início (YYYY-MM-DD) a ser considerada na busca dos dados
                        end_date (datetime)   : Data fim (YYYY-MM-DD) a ser considerada na busca dos dados
            Retorno:    params (List)         : Lista com os parâmetros a serem utilizados na requisição 
    """    
    date_aux = start_date
    params=[]
    while(date_aux <= end_date):
        params.append({'start_date':date_aux.date(),'end_date':date_aux.date(),'api_key':api_key})
        date_aux = date_aux + timedelta(days=1) 
    return params

def main():
    """
    Função principal.
            Parametros:
                    API_KEY (str)    : Chave utilizada nas requisições para a api
                    START_DATE (str) : Data início (YYYY-MM-DD) a ser considerada na busca dos dados
                    END_DATE (str)   : Data fim (YYYY-MM-DD) a ser considerada na busca dos dados
            Retorno: None
    """
    API_KEY     = sys.argv[1]
    START_DATE  = sys.argv[2]
    END_DATE    = sys.argv[3]
    
    #sessão spark
    spark = SparkSession.builder.appName("extract_asteroids_data").getOrCreate()
    
    #parametros da requisicao
    params = generate_request_params(API_KEY
                                    ,datetime.strptime(START_DATE, '%Y-%m-%d')
                                    ,datetime.strptime(END_DATE, '%Y-%m-%d'))
    #Requisições paralelas
    rdd = spark.sparkContext.parallelize(params) \
          .map(lambda param: get_data_asteroid(params=param))
    
    #transforma o rdd em dataframe
    df = transform_data(spark, rdd)
    
    #exibe o schema do dataframe
    df.printSchema()
    
    #exibe o dataframe
    df.show()

    return None

if __name__ == "__main__":
        main()