# Desafio Técnico - Plataform Builders
Bem-vindo ao repositório do desafio técnico para a vaga de Data Analyst na Plataform Builders. 
O Desafio consistia em desenvolver o ETL de duas fontes de dados diferentes (MongoDB e Mysql) e após o tratamento  realizar a construção de dashboards (https://platformbuilders.notion.site/Desafio-T-cnico-09deaa73a4c64350863be5ea6a91feba)

## ETL
O processo de ETL foi desenvolvido com python, criando conexões com ambos os bancos tratando e descarregando os dados como .csv (O load dos dados foi feito como csv por não ter permissão de criação de tabelas no banco Mysql, que era minha primeira opção)
  - index.py
  - convid_etl.py
  - multas_etl.py
  - mongo_db.py
  - mysql_db.py

## Diagramas
Foi desenvolvido um diagrama para cada um dos star schemas criados 
  - diagrama_covid.drawio
  - diagrama_multa.drawio

## Dashboards
Os dashboards foram desenvolvidos com Looker Studio
  -https://lookerstudio.google.com/reporting/a066482d-d6ca-4066-acd7-a1153cfa65ba
