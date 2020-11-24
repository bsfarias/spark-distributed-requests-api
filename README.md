# SPARK PARALLEL API REQUESTS

## Introdução
Este projeto foi criado com a finalidade de explorar o comando map, do Apache Spark, a fim de fazer requisições paralelas a uma [API](https://api.nasa.gov/).

##  Descrição
Utiliza-se um Job Spark para fazer requisições get de forma distribuída através da função map e, após processamento, um dataframe é criado. O Número de requisições é definido pela quantidade de dias a serem considerados na busca; o range de data deve ser informado no submit do job (<Start Date> <End Date>). 

## Pré-requisitos:
* [docker](https://www.docker.com/products/docker-desktop)
* Chave para acessar a [API](https://api.nasa.gov/)

## Construção do ambiente através do docker compose:
   - No terminal, Execute o seguinte comando:
```
cd spark-parallel-api-requets/docker/
docker-compose up
```   

## Execução do job spark:
   - Acesse o container e faça o submit do job spark:
```
docker exec -i -t spark /bin/bash
spark-submit --master local[*] /home/jovyan/scripts/parallel_requests.py --api_key <api_key> --start_date <start_date> --end_date <end_date>
```