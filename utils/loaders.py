import glob
import os
import pandas as pd

def arquivo_mais_recente(padrao):
    arquivos = glob.glob(padrao)
    if not arquivos:
        return None
    return max(arquivos, key=os.path.getctime)

def carregar_comparacao():
    arq = arquivo_mais_recente("data/comparacoes/comparacao_categorias_*.csv")
    if not arq:
        return pd.DataFrame()
    return pd.read_csv(arq, sep=';', encoding='utf-8-sig')

def carregar_amazon_sample():
    arq = arquivo_mais_recente("data/resultados_amazon/mais_vendidos_amazon_SAMPLE_*.csv")
    if not arq:
        return pd.DataFrame()
    return pd.read_csv(arq, sep=';', encoding='utf-8-sig')

def carregar_amazon_full():
    arq = arquivo_mais_recente("data/resultados_amazon/mais_vendidos_amazon_FULL_*.csv")
    if not arq:
        return pd.DataFrame()
    return pd.read_csv(arq, sep=';', encoding='utf-8-sig')

def carregar_ml_sample():
    arq = arquivo_mais_recente("data/resultados_ml/mais_vendidos_ml_SAMPLE_*.csv")
    if not arq:
        return pd.DataFrame()
    return pd.read_csv(arq, sep=';', encoding='utf-8-sig')

def carregar_ml_full():
    arq = arquivo_mais_recente("data/resultados_ml/mais_vendidos_ml_FULL_*.csv")
    if not arq:
        return pd.DataFrame()
    return pd.read_csv(arq, sep=';', encoding='utf-8-sig')

def carregar_todos_amazon():
    arquivos = glob.glob("data/resultados_amazon/mais_vendidos_amazon_*.csv")
    if not arquivos:
        return pd.DataFrame()
    dfs = [pd.read_csv(a, sep=";", encoding="utf-8-sig") for a in arquivos]
    return pd.concat(dfs, ignore_index=True)

def carregar_todos_ml():
    arquivos = glob.glob("data/resultados_ml/mais_vendidos_ml_*.csv")
    if not arquivos:
        return pd.DataFrame()
    dfs = [pd.read_csv(a, sep=";", encoding="utf-8-sig") for a in arquivos]
    return pd.concat(dfs, ignore_index=True)


# --------- novos helpers adicionados pela solicitação ------------

def arquivos_ultimos_n(padrao, n=5):
    """Retorna os *n* arquivos mais recentes que batem no padrão especificado.

    A ordenação é feita por data de modificação (ctime). Se houver menos do que
    *n* arquivos, todos são retornados.  O arquivo mais recente aparece primeiro.
    """
    arquivos = glob.glob(padrao)
    arquivos.sort(key=os.path.getctime, reverse=True)
    return arquivos[:n]


def precos_por_nome(arquivos, nome_col="Nome", preco_col="Preço"):
    """Cria um dicionário nome->preço usando a última aparição em *arquivos*.

    Percorre os arquivos em ordem cronológica (mais antigos primeiro) para que a
    última ocorrência de um mesmo nome sobrescreva as anteriores. Essa ordem faz
    sentido quando queremos "a última aparição do nome" entre várias versões.
    """
    preco_dict = {}
    arquivos_ordenados = sorted(arquivos, key=os.path.getctime)
    for arq in arquivos_ordenados:
        try:
            df = pd.read_csv(arq, sep=";", encoding="utf-8-sig")
        except Exception:
            continue
        if nome_col not in df.columns or preco_col not in df.columns:
            continue
        for nome, preco in zip(df[nome_col].astype(str), df[preco_col].astype(str)):
            preco_dict[nome] = preco
    return preco_dict


def carregar_comparacao_master_com_precos():
    """Carrega o último arquivo *MASTER* de comparações e anota preços.

    - Encontra o último `comparacao_categorias_MASTER_*.csv` em
      `data/comparacoes`
    - Busca os últimos 5 arquivos **sample** de cada plataforma
      (`amazon_SAMPLE`, `ml_SAMPLE`).
    - Para cada conjunto constroi um dicionário nome->preço usando a última
      ocorrência do campo `Nome` nas versões consideradas.
    - Associa os preços ao `Produto Amazon` e ao `Produto Mercado Livre` da
      comparação final, inserindo as colunas `Preço Prod Amazon` e
      `Preço Prod ML` imediatamente antes das colunas de link.
    """
    padrao_master = "data/comparacoes/comparacao_categorias_MASTER_*.csv"
    arquivos_master = glob.glob(padrao_master)
    if not arquivos_master:
        return pd.DataFrame()
    arq_master = max(arquivos_master, key=os.path.getctime)
    df = pd.read_csv(arq_master, sep=";", encoding="utf-8-sig")

    # obtém os últimos cinco arquivos "sample" de cada plataforma
    amz_sample = arquivos_ultimos_n("data/resultados_amazon/mais_vendidos_amazon_SAMPLE_*.csv", 5)
    ml_sample = arquivos_ultimos_n("data/resultados_ml/mais_vendidos_ml_SAMPLE_*.csv", 5)

    amz_dict = precos_por_nome(amz_sample, nome_col="Nome", preco_col="Preço")
    # no ML o campo chama-se "Preço à vista" (observado nos samples)
    ml_dict = precos_por_nome(ml_sample, nome_col="Nome", preco_col="Preço à vista")

    df["Produto Amazon"] = df["Produto Amazon"].astype(str)
    df["Produto Mercado Livre"] = df["Produto Mercado Livre"].astype(str)
    df["Preço Prod Amazon"] = df["Produto Amazon"].map(amz_dict).fillna("")
    df["Preço Prod ML"] = df["Produto Mercado Livre"].map(ml_dict).fillna("")

    # reposiciona colunas antes dos links
    cols = list(df.columns)
    if "Preço Prod Amazon" in cols and "Link Amazon" in cols:
        cols.remove("Preço Prod Amazon")
        idx = cols.index("Link Amazon")
        cols.insert(idx, "Preço Prod Amazon")
    if "Preço Prod ML" in cols and "Link Mercado Livre" in cols:
        cols.remove("Preço Prod ML")
        idx = cols.index("Link Mercado Livre")
        cols.insert(idx, "Preço Prod ML")
    df = df[cols]

    return df
