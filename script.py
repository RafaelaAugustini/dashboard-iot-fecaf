import os
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "12345")  
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "postgres")

CSV_PATH = Path("data/IOT-temp.csv") 
TABLE_NAME = "banco_fecaf"

if not CSV_PATH.exists():
    print(f"Erro: arquivo CSV não encontrado em: {CSV_PATH.resolve()}")
    sys.exit(1)

# engine SQLAlchemy
engine_url = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(engine_url, future=True)

# lê o csv
df = pd.read_csv(CSV_PATH)
print("Colunas originais do CSV:", list(df.columns))

# colunas
cols = [c.lower().strip() for c in df.columns]

# mapeamentos 
mapping = {}
for orig, low in zip(df.columns, cols):
    if "device" in low or "log" in low or "__export__" in low:
        mapping[orig] = "device_id"
    elif "temp" in low and "date" not in low:
        mapping[orig] = "temperature"
    elif "value" == low or low in ("val", "reading"):
        mapping[orig] = "temperature"
    elif "time" in low or "date" in low or "timestamp" in low:
        mapping[orig] = "timestamp"
    elif "room" in low or "location" in low:
        mapping[orig] = "room"
    elif "status" in low or low in ("in","out"):
        mapping[orig] = "status"
    elif low.isdigit() or low == "id":
        # se coluna é id/indice
        mapping[orig] = "id"

# aplicando mapeamento
df = df.rename(columns=mapping)
print("Mapping aplicado (se houver):", mapping)
print("Colunas após rename:", list(df.columns))

if "temperature" not in df.columns:
    # tenta achar a primeira coluna numérica com médias plausíveis (ex:  -50 a 150)
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    found = False
    for c in numeric_cols:
        vals = df[c].dropna()
        if not vals.empty and vals.between(-50, 150).mean() > 0.5:
            df = df.rename(columns={c: "temperature"})
            print(f"Inferido '{c}' como 'temperature'")
            found = True
            break
    if not found:
        print("Aviso: não foi possível inferir uma coluna 'temperature' automaticamente. Você pode renomear manualmente.")
        print(df.head())
if "device_id" not in df.columns:
    for c in df.columns:
        if df[c].dtype == object and df[c].str.len().median() > 5:
            df = df.rename(columns={c: "device_id"})
            print(f"Inferido '{c}' como 'device_id'")
            break

if "timestamp" in df.columns:
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

if "timestamp" not in df.columns:
    # tenta encontrar colunas com 'date' e 'time'
    date_cols = [c for c in df.columns if "date" in c.lower()]
    time_cols = [c for c in df.columns if "time" in c.lower()]
    if date_cols:
        col = date_cols[0]
        df["timestamp"] = pd.to_datetime(df[col], errors="coerce")
        print(f"Usando '{col}' como timestamp (tentativa)")

# exibe as colunas finais
print("Colunas finais importantes:", [c for c in ["device_id","timestamp","temperature","room","status"] if c in df.columns])

# salva os dados no banco
print(f"Enviando dataframe para o Postgres (tabela: {TABLE_NAME})...")
df.to_sql(name=TABLE_NAME, con=engine, index=False, if_exists="replace", method="multi")
print("Upload finalizado.")

# criando as views
create_view_queries = [
    f"""
    CREATE OR REPLACE VIEW avg_temp_por_dispositivo AS
    SELECT device_id,
           AVG(temperature)::numeric(8,2) AS avg_temp,
           COUNT(*) AS total_leituras
    FROM {TABLE_NAME}
    WHERE temperature IS NOT NULL
    GROUP BY device_id
    ORDER BY device_id;
    """,
    f"""
    CREATE OR REPLACE VIEW leituras_por_hora AS
    SELECT EXTRACT(HOUR FROM timestamp) AS hora,
           COUNT(*) AS contagem
    FROM {TABLE_NAME}
    WHERE timestamp IS NOT NULL
    GROUP BY hora
    ORDER BY hora;
    """,
    f"""
    CREATE OR REPLACE VIEW temp_max_min_por_dia AS
    SELECT DATE(timestamp) AS data,
           MAX(temperature) AS temp_max,
           MIN(temperature) AS temp_min,
           AVG(temperature)::numeric(8,2) AS temp_media
    FROM {TABLE_NAME}
    WHERE timestamp IS NOT NULL AND temperature IS NOT NULL
    GROUP BY DATE(timestamp)
    ORDER BY DATE(timestamp);
    """
]

with engine.begin() as conn:  
    for q in create_view_queries:
        conn.execute(text(q))

with engine.connect() as conn:
    res = conn.execute(text("SELECT * FROM avg_temp_por_dispositivo LIMIT 10;"))
    for row in res.fetchall():
        print(row)

with engine.connect() as conn:
    res = conn.execute(text("SELECT * FROM leituras_por_hora LIMIT 24;"))
    for row in res.fetchall():
        print(row)

with engine.connect() as conn:
    res = conn.execute(text("SELECT * FROM temp_max_min_por_dia LIMIT 10;"))
    for row in res.fetchall():
        print(row)
