from mysql_db import MySQL
from datetime import datetime
import json
import csv

sql = MySQL()




class Covid_ETL:

    def transforme(self):           
        self.state = self.table_state()
        self.city = self.table_city()
        self.report = self.table_report_covid()
        print(f"Sucess ETL Covid ")
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

    def table_state(self):
        query = "SELECT  DISTINCT(city_ibge_code),state,estimated_population_2019, estimated_population FROM teste_dados.DADOS_COVID WHERE place_type = %s" 
        values = ('state',)
        states_row = sql.return_query(query = query,value =values)
        states_trat = []
        for state in states_row:
            states_trat.append({
                "state_ibge_code" : state[0],
                "sigla" : state[1],
                "estimated_population_2019" : state[2],
                "estimated_populatio" : state[3]
            })
        self.writer_csv("state.csv",states_trat)
        return states_trat

    def table_city(self):
        query = "SELECT  DISTINCT(city_ibge_code),city,estimated_population_2019, estimated_population FROM teste_dados.DADOS_COVID WHERE place_type = %s" 
        values = ('city',)
        citys_row = sql.return_query(query = query,value =values)
        citys_trat = []
        for city in citys_row:
            citys_trat.append({
                "city_ibge_code" : city[0],
                "state_ibge_code":int(str(city[0])[0:2]),
                "name" : (city[1].encode('latin-1')).decode('utf-8'),
                "estimated_population_2019" : city[2],
                "estimated_populatio" : city[3]
            })
        self.writer_csv("citys.csv",citys_trat)
        return citys_trat

    def table_report_covid(self):
        query = 'SELECT  city_ibge_code, date, epidemiological_week, is_last, is_repeated, last_available_confirmed, last_available_confirmed_per_100k_inhabitants, last_available_date, last_available_death_rate, last_available_deaths, order_for_place, new_confirmed, new_deaths FROM teste_dados.DADOS_COVID '
        reports_row = sql.return_query(query = query,value = None)
        reports_trat = []
        for report in reports_row:
            reports_trat.append({
                "city_ibge_code" : report[0] if report[0] > 100 else 0,
                "state_ibge_code" : report[0] if report[0]< 100 else 0,
                "date" :str(datetime.strptime(report[1], '%Y-%m-%d').date()),
                "epidemiological_week" :str(report[2])[0:4] +'-' +str(report[2])[4:],
                "is_last" : False if report[3] =='False' else True ,
                "is_repeated" : False if report[4] == 'False' else True,
                "last_available_confirmed" :report[5],
                "last_available_confirmed_per_100k_inhabitants" :report[6],
                "last_available_date" :str(datetime.strptime(report[7], '%Y-%m-%d').date()),
                "last_available_death_rate" : report[8],
                "last_available_deaths" : report[9],
                "order_for_place" : report[10],
                "new_confirmed" : report[11],
                "new_deaths" : report[12]
            })
        self.writer_csv("reports.csv",reports_trat)
        return reports_trat