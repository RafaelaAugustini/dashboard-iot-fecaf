import os
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine, text

PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "12345")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "postgres")

engine_url = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(engine_url, future=True)

st.set_page_config(page_title="Dashboard de Temperaturas IoT", layout="wide")

st.title("Dashboard de Temperaturas IoT UNIFECAF")
st.markdown("Visualizações baseadas em views SQL: `avg_temp_por_dispositivo`, `leituras_por_hora`, `temp_max_min_por_dia`")

@st.cache_data(ttl=300)
def load_view(view_name):
    try:
        df = pd.read_sql(text(f"SELECT * FROM {view_name}"), engine)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar view `{view_name}`: {e}")
        return pd.DataFrame()

# gráfico 1
st.header("Média de Temperatura por Dispositivo")
df_avg = load_view("avg_temp_por_dispositivo")
if not df_avg.empty:
    fig1 = px.bar(df_avg, x="device_id", y="avg_temp", hover_data=["total_leituras"])
    st.plotly_chart(fig1, use_container_width=True)
else:
    st.info("View `avg_temp_por_dispositivo` vazia ou inexistente.")

# gráfico 2
st.header("Leituras por Hora do Dia")
df_hora = load_view("leituras_por_hora")
if not df_hora.empty:
    if "hora" in df_hora.columns:
        df_hora["hora"] = df_hora["hora"].astype(int)
    fig2 = px.line(df_hora.sort_values("hora"), x="hora", y="contagem", markers=True)
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("View `leituras_por_hora` vazia ou inexistente.")

# gráfico 3
st.header("Temperaturas Máximas e Mínimas por Dia")
df_day = load_view("temp_max_min_por_dia")
if not df_day.empty:
    if "data" in df_day.columns:
        df_day["data"] = pd.to_datetime(df_day["data"])
        fig3 = px.line(df_day.sort_values("data"), x="data", y=["temp_max", "temp_min", "temp_media"])
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.write(df_day.head())
else:
    st.info("View `temp_max_min_por_dia` vazia ou inexistente.")

# visualização da tabela bruta 
with st.expander("Mostrar tabela bruta (banco_fecaf)"):
    try:
        df_raw = pd.read_sql(text("SELECT * FROM banco_fecaf LIMIT 1000"), engine)
        st.dataframe(df_raw)
    except Exception as e:
        st.error(f"Erro ao carregar tabela `banco_fecaf`: {e}")
