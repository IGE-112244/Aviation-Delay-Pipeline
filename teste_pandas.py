import pandas as pd

# Carregar o dataset
df = pd.read_csv("data/raw/Airlines.csv")

# Quantas linhas e colunas tem?
print("--- Dimensão do dataset ---")
print(f"Linhas: {len(df):,}")
print(f"Colunas: {len(df.columns)}")

# Quais são as colunas?
print("\n--- Colunas ---")
print(df.columns.tolist())

# Que tipo de dados tem cada coluna?
print("\n--- Tipos de dados ---")
print(df.dtypes)

# Ver as primeiras 5 linhas
print("\n--- Primeiras 5 linhas ---")
print(df.head())

# Há valores nulos?
print("\n--- Valores nulos por coluna ---")
print(df.isnull().sum())

# Estatísticas básicas das colunas numéricas
print("\n--- Estatísticas básicas ---")
print(df.describe())

# Quantas companhias aéreas diferentes existem?
print("\n--- Companhias aéreas ---")
print(df["Airline"].value_counts())

# Qual a taxa de atraso por companhia?
print("\n--- Taxa de atraso por companhia (%) ---")
atraso_por_companhia = df.groupby("Airline")["Delay"].mean() * 100
print(atraso_por_companhia.sort_values(ascending=False).round(1))

# Distribuição de voos por dia da semana
print("\n--- Voos por dia da semana ---")
print(df["DayOfWeek"].value_counts().sort_index())

# Taxa de atraso por dia da semana
print("\n--- Taxa de atraso por dia da semana (%) ---")
atraso_dia = df.groupby("DayOfWeek")["Delay"].mean() * 100
print(atraso_dia.round(1))

# Voos com duração zero - o outlier que identificámos
print("\n--- Voos com duração zero ---")
print(len(df[df["Length"] == 0]))