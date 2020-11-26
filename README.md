# SPARK DISTRIBUTED REQUESTS API

## Introdução
Este projeto foi criado com a finalidade de explorar o comando map, do Apache Spark, a fim de fazer requisições distribuídas a uma [API](https://api.nasa.gov/).

## Pré-requisitos:
* [docker](https://www.docker.com/products/docker-desktop)
* Chave para acessar a [API](https://api.nasa.gov/)

## Construção do ambiente através do docker compose:
   - Na pasta raiz do projeto, execute o seguinte comando:
```
docker-compose up
```   

## Acesso ao container:
   - No terminal, execute:
```
docker exec -i -t spark /bin/bash
```   

## Teste unitário:
   - Dentro do container, execute:
```
export API_KEY=<SUA API KEY>

cd /home/jovyan/scripts/

python3 -m unittest tests/test_distributed_requests.py
```
   - Obs: substitua `<SUA API KEY>` pela sua real api_key (verificar Pré-requisitos).

## Execução do job spark:
   - No do container, execute:
```
docker exec -i -t spark /bin/bash

spark-submit --master local[*] \
/home/jovyan/scripts/jobs/distributed_requests.py \
--api_key <api_key> \
--start_date <start_date> \
--end_date <end_date>
```