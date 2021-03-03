#### etl-covid19-brasil
# Estudo ETL sobre o Covid-19 no Brasil

## O que este projeto faz

Este projeto cria um data lake com os dados da planilha copilada do site https://brasil.io sobre o Covid-19 no Brasil. Acessa o IBGE para pegar as informações sobre os municípios e também adiciona essas informações ao data lake.

O projeto utiliza esse data lake criado para consolidar os dados de ambas informações (covid-19 e municípios) e salva esses dados em uma base ElasticSearch, um data mart, para que seja utilizada em um painel de informações.

Este projeto está utilizando as seguintes tecnologias:

- Apache Airflow 2
- Pandas
- Numpy
- SqlAlchemy
- ElasticSearch
- Eland
- Docker

</br>
</br>
</br>

## Executando o projeto:

Inicialmente é necessário que seja efetuado o download do arquivo de casos de covid-19 no Brasil em: https://brasil.io/dataset/covid19/files/

Escolha a opção ```caso_full.csv.gz```, descompacte o arquivo e o adicione dentro da pasta ```data``` na raiz do projeto.

Para executar este projeto é necessário que **tenha o Docker instalado** em sua máquina.

</br>

1 - Na raiz do projeto, no terminal, utilize os comandos abaixo para rodar:

Versão local:
```
    docker-compose -f docker-compose-local.yml up -d
```
Versão de produção:

```
    docker-compose -f docker-compose-production.yml up -d
```

Caso queira instanciar mais de um worker utilize:
```
  docker-compose -f docker-compose-production.yml up -d scale worker=5
```

</br>

2 - Para validar a subida, acesse em seu navegador o endereço ```localhost:8080``` para abrir o painel do Airflow.

</br>

3 - Para que todo o processo de ETL funcione corretamente é necessário criar as variáveis de ambiente diretamente no airflow, para isso utilize o arquivo _variables_example.json_, preencha os dados necessário e o importe no painel do airflow para alimentar todas as variáveis do projeto.

Agora basta ligar e executar a DAG.

</br>
</br>
</br>

## DAGs

Esse projeto possui duas DAGs:

</br>

### create_data_lake

Que busca os dados no arquivo csv com os dados de casos de Covid-19 no Brasil e salva em uma base Postgresql.
Busca os dados de municípios na API do IBGE e salva em uma base Postgresql para simular um data lake em base relacional.

</br>

### etl-covid

Busca os dados dos casos de Covid-19 e dos Munícipios que estão na base relacional Postgresql, consolida os casos e salva esses dados consolidados em uma base ElasticSearch para simular um data mart.

</br>
</br>
</br>

## Para configurar o AirFlow

Nessa implementação foi utilizado a imagem https://github.com/puckel/docker-airflow neste repositório existe toda a documentação com os detalhes para as diversas configurações.

</br>
</br>
</br>

## Libs Python utilizadas:

- pandas
- numpy
- loggin
- psycopg2
- sqlalchemy
- datetime
- requests
- json
- elasticsearch
- eland
