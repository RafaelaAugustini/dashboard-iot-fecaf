# Pipeline IoT – UNIFECAF

Projeto que lê dados de temperatura de sensores IoT, grava no PostgreSQL e mostra gráficos no Streamlit.

## Pré-requisitos:
- Python 3
- Docker
- CSV em data/IOT-temp.csv

## Como inicializar o projeto localmente:

1. **Iniciar PostgreSQL**  

- docker run --name postgres-iot -e POSTGRES_PASSWORD=12345 -p 5432:5432 -d postgres

2. **Instalar as dependências necessárias**

- pip install pandas psycopg2-binary sqlalchemy streamlit plotly

3. **Rodar o pipeline**

- python script.py

4. **Rodar o dashboard**

- streamlit run dashboard.py
