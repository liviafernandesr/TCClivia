import pandas as pd
import re, unidecode, os, glob
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# pegando os arquivos mais recentes
def arquivo_mais_recente(padrao):
    arquivos = glob.glob(padrao)
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para o padrão: {padrao}")
    return max(arquivos, key=os.path.getctime)

amazon_csv = arquivo_mais_recente("resultados_amazon/mais_vendidos_amazon_SAMPLE_*.csv")
ml_csv = arquivo_mais_recente("resultados/mais_vendidos_ml_SAMPLE_*.csv")

print("📄 Arquivo Amazon:", amazon_csv)
print("📄 Arquivo Mercado Livre:", ml_csv)

# carregando
amazon = pd.read_csv(amazon_csv, sep=';', encoding='utf-8-sig')
mercadolivre = pd.read_csv(ml_csv, sep=';', encoding='utf-8-sig')


col_amazon = 'Nome' if 'Nome' in amazon.columns else amazon.columns[0]
col_ml = 'Nome' if 'Nome' in mercadolivre.columns else mercadolivre.columns[0]

# limpando os textos
def limpar_texto(texto):
    texto = str(texto).lower()
    texto = unidecode.unidecode(texto)
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    return texto.strip()

amazon[col_amazon] = amazon[col_amazon].apply(limpar_texto)
mercadolivre[col_ml] = mercadolivre[col_ml].apply(limpar_texto)

# criando vetores
vectorizer = TfidfVectorizer()
tfidf_amazon = vectorizer.fit_transform(amazon[col_amazon])
tfidf_ml = vectorizer.transform(mercadolivre[col_ml])

similarity_matrix = cosine_similarity(tfidf_amazon, tfidf_ml)

# procurando matches
matches = []
for i, produto_amz in enumerate(amazon[col_amazon]):
    idx_best = similarity_matrix[i].argmax()
    best_match = mercadolivre.loc[idx_best, col_ml]
    score = similarity_matrix[i, idx_best]
    matches.append({
        "Produto Amazon": produto_amz,
        "Melhor correspondência ML": best_match,
        "Similaridade": round(score, 3)
    })

resultados = pd.DataFrame(matches)

# filtrando por similaridade mínima
limiar = 0.8 
resultados_filtrados = resultados[resultados["Similaridade"] >= limiar]

print("\n🧾 Resultados filtrados:")
print(resultados_filtrados)


saida_csv = f"resultados/comparacao_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv"
resultados_filtrados.to_csv(saida_csv, index=False, sep=';', encoding='utf-8-sig')
print(f"\n✅ Resultado salvo em: {saida_csv}")
