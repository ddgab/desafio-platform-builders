from multas_etl import Multas_ETL
from covid_etl import Covid_ETL




def main():

    covid = Covid_ETL()
    print(f"Start ETL Covid")
    covid_transforme = covid.transforme()

    # multas = Multas_ETL()
    # print(f"Start ETL Multas")
    # multas_transforme =  multas.transforme()




if __name__ == "__main__":
    main()