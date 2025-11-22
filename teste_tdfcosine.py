import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# teste 1
amazon = pd.DataFrame({
    "produto_amazon": [
        "Smartphone Samsung Galaxy S23 128GB Preto",
        "Notebook Lenovo Ideapad 3 15.6 polegadas",
        "Fone Bluetooth JBL Tune 510BT Azul"
    ]
})

mercadolivre = pd.DataFrame({
    "produto_ml": [
        "Celular Samsung Galaxy S23 128 GB cor preta",
        "Notebook Lenovo Ideapad 3 15,6 polegadas Ryzen 5",
        "Fone sem fio JBL Tune 510BT cor azul"
    ]
})

import re, unidecode
def limpar_texto(texto):
    texto = unidecode.unidecode(texto.lower())
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    return texto
amazon["produto_amazon"] = amazon["produto_amazon"].apply(limpar_texto)
mercadolivre["produto_ml"] = mercadolivre["produto_ml"].apply(limpar_texto)


# criando a matriz TF-IDF, vetores
vectorizer = TfidfVectorizer(stop_words=None)
tfidf_amazon = vectorizer.fit_transform(amazon["produto_amazon"])
tfidf_ml = vectorizer.transform(mercadolivre["produto_ml"])


# calculando a similaridade do cosseno
similarity_matrix = cosine_similarity(tfidf_amazon, tfidf_ml)

# encontrando os pares
matches = []
for i, produto_amz in enumerate(amazon["produto_amazon"]):
    idx_best = similarity_matrix[i].argmax()
    best_match = mercadolivre.loc[idx_best, "produto_ml"]
    score = similarity_matrix[i, idx_best]
    matches.append({
        "Produto Amazon": produto_amz,
        "Melhor correspondência ML": best_match,
        "Similaridade": round(score, 3)
    })

resultados = pd.DataFrame(matches)

# limiar de similaridade
# 0.7–0.8 → bom equilíbrio entre precisão e cobertura
# Abaixo de 0.7 → pode gerar falsos positivos

limiar = 0.75
resultados_filtrados = resultados[resultados["Similaridade"] >= limiar]

print(resultados_filtrados)
