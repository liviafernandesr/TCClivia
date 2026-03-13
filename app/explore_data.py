import pandas as pd
import json

# Explorar Amazon FULL - apenas primeiras 5 colunas e 1 linha
df_amz = pd.read_csv('data/resultados_amazon/mais_vendidos_amazon_FULL_2026-02-d_07-31.csv', sep=';', encoding='utf-8-sig', nrows=1)
print("Amazon FULL - Número de colunas:", len(df_amz.columns))
print("Primeiras 10 colunas:", df_amz.columns.tolist()[:10])
print("\n")

# Explorar ML FULL
df_ml = pd.read_csv('data/resultados_ml/mais_vendidos_ml_FULL_2026-02-d_22-53.csv', sep=';', encoding='utf-8-sig', nrows=1)
print("ML FULL - Número de colunas:", len(df_ml.columns))
print("Primeiras 10 colunas:", df_ml.columns.tolist()[:10])
print("\n")

# Verificar se há colunas com "comentário" ou "avaliação" ou "review"
print("Procurando colunas com 'comentário', 'review', 'avaliação', 'estrela':")
for col in df_amz.columns:
    if any(x in col.lower() for x in ['comentário', 'review', 'avaliação', 'estrela', 'comentario']):
        print(f"  Amazon: {col}")

for col in df_ml.columns:
    if any(x in col.lower() for x in ['comentário', 'review', 'avaliação', 'estrela', 'comentario']):
        print(f"  ML: {col}")

# Mostrar um sample de uma linha da Amazon
print("\n\nSample de uma linha da Amazon:")
print(df_amz.iloc[0].to_dict())

print("\n\nSample de uma linha do ML:")
print(df_ml.iloc[0].to_dict())
