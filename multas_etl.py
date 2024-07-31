import json
import csv
from mongo_db import MongoDB
from datetime import datetime
mongo = MongoDB()
collection_multas = mongo.define_db('multas', 'teste_dados')
meses = {
    "janeiro": 1,
    "fevereiro": 2,
    "março": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12
}

tables_values = (
     {
        "table_name":"uf",
        "column_name":"uf"
    },
    {
        "table_name":"escopo_autuacao",
        "column_name":"escopo_autuacao"
    },
    {
        "table_name":"amparo_legal",
        "column_name":"amparo_legal"
    },
    {
        "table_name":"infracao",
        "column_name":"descricao_infracao"
    }
)

class Multas_ETL:
    def __init__(self):
        self.pk_values = {}
    
    def transforme(self):     
        for table in tables_values:
            self.tables_generic = self.create_table_generic(table["table_name"],table["column_name"])

        self.relatorio_autuacao = self.table_relatorio_autuacao()
        print(f"Sucess ETL Multas ")
        return True

    def writer_csv(self,name_doc,data): 
        try:       
            with open(name_doc,mode ='a',newline='',encoding='utf-8-sig') as csv_file:     
                writer = csv.DictWriter(csv_file, fieldnames=  data[0].keys())
                writer.writeheader()
                writer.writerows(data)  
            print(f"Create table {name_doc.split('.csv')[0]} csv")
        except Exception as e:
            error = f"{type(e).__module__}:{type(e).__name__}: {str(e).rstrip()}"
            print(error)
        return True

    
    def create_table_generic(self,table_name,column_name):
        self.pk_values[table_name] = {}
        values_row = collection_multas.distinct(column_name)
        values_trat = []
        id = 0
        for values in values_row:   
            id += 1
            values_trat.append({
                "id_"+table_name : id,
                column_name :values
            })
            self.pk_values[table_name][values] = id
        self.writer_csv(table_name+".csv",values_trat)
        return True
    
    def table_relatorio_autuacao(self):
        self.pk_values["relatorio_autuacao"] = {}
        relatorio_autuacao_row = collection_multas.find({})
        relatorio_autuacao_trat = []
        for relatorio_autuacao in relatorio_autuacao_row: 
            relatorio_autuacao_trat.append({
                "id_escopo_autuacao":self.pk_values["escopo_autuacao"][relatorio_autuacao["escopo_autuacao"]],
                "id_uf":self.pk_values["uf"][relatorio_autuacao["uf"]],
                "id_amparo_legal":self.pk_values["amparo_legal"][relatorio_autuacao["amparo_legal"]] if "amparo_legal" in relatorio_autuacao else None,
                "id_infracao":self.pk_values["infracao"][relatorio_autuacao["descricao_infracao"]],
                "date":str(datetime(int(relatorio_autuacao["ano"]), meses[relatorio_autuacao["mes"].lower()], 1).date()),
                'quantidade_autos':int(relatorio_autuacao["quantidade_autos"])                
            })                
        self.writer_csv("relatorio_autuacao.csv",relatorio_autuacao_trat)
        return True




