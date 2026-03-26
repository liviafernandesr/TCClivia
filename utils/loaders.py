import glob
import os
import re
import pandas as pd


def norm_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def _timestamp_no_nome(path: str):
    """Extrai timestamp do padrão YYYY-MM-DD_HH-MM no nome do arquivo."""
    nome = os.path.basename(path)
    m = re.search(r"(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2})", nome)
    if not m:
        return None
    return (m.group(1), m.group(2))


def _chave_recencia(path: str):
    """Prioriza timestamp do nome; fallback para ctime."""
    ts = _timestamp_no_nome(path)
    if ts is not None:
        return (1, ts)
    try:
        return (0, os.path.getctime(path))
    except Exception:
        return (0, 0)

def arquivo_mais_recente(padrao):
    arquivos = glob.glob(padrao)
    if not arquivos:
        return None
    return max(arquivos, key=_chave_recencia)

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
    dfs = [pd.read_csv(a, sep=';', encoding='utf-8-sig') for a in arquivos]
    return pd.concat(dfs, ignore_index=True)


def carregar_ultimos_full(padrao, n=5):
    """Retorna lista de (df, ctime, path) para os últimos *n* arquivos que batem no padrão.

    A lista é ordenada do mais recente para o mais antigo.
    """
    arquivos = arquivos_ultimos_n(padrao, n)
    resultado = []
    for arq in arquivos:
        try:
            df = pd.read_csv(arq, sep=";", encoding="utf-8-sig")
        except Exception:
            continue
        try:
            ctime = os.path.getctime(arq)
        except Exception:
            ctime = 0
        resultado.append((df, ctime, arq))
    # already arquivos_ultimos_n returns most recent first; ensure ordering
    resultado.sort(key=lambda x: x[1], reverse=True)
    return resultado


def carregar_ultimos_amazon_full(n=5):
    return carregar_ultimos_full("data/resultados_amazon/mais_vendidos_amazon_FULL_*.csv", n)


def carregar_ultimos_ml_full(n=5):
    return carregar_ultimos_full("data/resultados_ml/mais_vendidos_ml_FULL_*.csv", n)


def _cache_dir():
    p = os.path.join("data", "cache")
    os.makedirs(p, exist_ok=True)
    return p


def _cache_path(platform):
    return os.path.join(_cache_dir(), f"comments_{platform}.csv")


def carregar_cache_comentarios(platform):
    path = _cache_path(platform)
    # ensure cache directory exists even if file is missing
    if not os.path.exists(path):
        # include link column for ML entries (may be blank for Amazon)
        return pd.DataFrame(columns=["produto", "texto", "nota", "data", "origem", "ctime", "link"])
    try:
        df = pd.read_csv(path, sep=';', encoding='utf-8-sig')
        # older caches might not have a "link" column; add it for consistency
        if 'link' not in df.columns:
            df['link'] = ''
        return df
    except Exception:
        return pd.DataFrame(columns=["produto", "texto", "nota", "data", "origem", "ctime", "link"])


def salvar_cache_comentarios(platform, df):
    path = _cache_path(platform)
    _cache_dir()
    df.to_csv(path, index=False, sep=';', encoding='utf-8-sig')


def atualizar_cache_comentarios(platform, n=5):
    """Atualiza (ou cria) o cache de comentários para `platform`.

    - Mantém comentários já existentes no cache.
    - Processa os últimos `n` FULLs da plataforma e adiciona/atualiza comentários
      no cache, preferindo as versões mais recentes quando há duplicatas de texto.
    - Para Mercado Livre, remapeia o campo de produto usando o nome presente no
      arquivo MASTER da comparação (coluna "Produto Mercado Livre"). Esse passo
      garante que a busca por comentários use os mesmos nomes exibidos na
      interface.

    Retorna o DataFrame atualizado do cache.
    """
    platform = platform.lower()
    if platform.startswith('am'):
        fulls = carregar_ultimos_amazon_full(n)
    else:
        fulls = carregar_ultimos_ml_full(n)

    # carregar cache pré-existente (pode não existir)
    cache = carregar_cache_comentarios(platform)
    # cache antigo de ML pode não ter coluna 'link' ou todos valores vazios;
    # nesse caso é melhor ignorá-lo e reconstruir do zero para garantir que os
    # nomes fiquem alinhados ao MASTER.
    if platform.startswith('ml') and not cache.empty:
        if 'link' in cache.columns and cache['link'].astype(str).str.strip().eq('').all():
            cache = pd.DataFrame(columns=cache.columns)
    # índice por texto para evitar duplicatas e permitir sobrescrever
    mapa = {str(r['texto']): r for _, r in cache.iterrows()} if not cache.empty else {}

    # construir mapeamento link->nome_master para ML (se aplicável)
    master_map = {}
    if platform.startswith('ml'):
        master_path = arquivo_mais_recente("data/comparacoes/comparacao_categorias_MASTER_*.csv")
        if master_path:
            try:
                mdf = pd.read_csv(master_path, sep=';', encoding='utf-8-sig')
                # normalizar nomes de coluna caso haja problemas de encoding
                rename_map = {
                    'Produto Mercado Livre': 'Produto Mercado Livre',
                    'Link Mercado Livre': 'Link Mercado Livre',
                }
                mdf = mdf.rename(columns=rename_map)
                if 'Link Mercado Livre' in mdf.columns and 'Produto Mercado Livre' in mdf.columns:
                    for _, mr in mdf.iterrows():
                        link = norm_str(mr.get('Link Mercado Livre', ''))
                        nome_ml = norm_str(mr.get('Produto Mercado Livre', ''))
                        if link and nome_ml:
                            master_map[link] = nome_ml
            except Exception:
                pass

    # processar do mais antigo para o mais recente para que os mais recentes
    # sobrescrevam entradas anteriores
    for df, ctime, path in reversed(fulls):
        if 'Nome' not in df.columns:
            continue
        for _, row in df.iterrows():
            texto = str(row.get('Comentário', '')).strip()
            if not texto:
                continue
            produto = str(row.get('Nome', '')).strip()
            link = str(row.get('Link', '')).strip() if 'Link' in row else ''
            # se tivermos um nome mestre mapeado para este link, use-o
            if platform.startswith('ml') and link and link in master_map:
                produto = master_map[link]
            nota = row.get('Nota Comentário', '')
            data = row.get('Data Comentário', '')
            key = texto
            mapa[key] = {
                'produto': produto,
                'texto': texto,
                'nota': nota,
                'data': data,
                'origem': path,
                'ctime': ctime,
                'link': link,
            }

    if mapa:
        novos = list(mapa.values())
        df_cache = pd.DataFrame(novos)
    else:
        df_cache = pd.DataFrame(columns=["produto", "texto", "nota", "data", "origem", "ctime", "link"])

    # aplicar remapeamento em toda tabela caso informações venham de cache antigo
    if platform.startswith('ml') and master_map:
        def map_produto(r):
            lk = norm_str(r.get('link', ''))
            return master_map.get(lk, r.get('produto', ''))
        df_cache['produto'] = df_cache.apply(map_produto, axis=1)

    salvar_cache_comentarios(platform, df_cache)
    return df_cache


def buscar_comentarios_cache_por_produto(platform, produto_nome):
    df = carregar_cache_comentarios(platform)
    if df.empty:
        return []
    nome_norm = norm_str(produto_nome).upper()
    def match(nome):
        return norm_str(nome).upper() == nome_norm
    df_filtrado = df[df['produto'].astype(str).apply(match)]
    resultados = []
    for _, r in df_filtrado.iterrows():
        resultados.append({
            'texto': r.get('texto', ''),
            'nota': r.get('nota', ''),
            'data': r.get('data', ''),
            'origem': r.get('origem', ''),
        })
    return resultados


# --------- novos helpers adicionados pela solicitação ------------

def arquivos_ultimos_n(padrao, n=5):
    """Retorna os *n* arquivos mais recentes que batem no padrão especificado.

    A ordenação é feita por data de modificação (ctime). Se houver menos do que
    *n* arquivos, todos são retornados.  O arquivo mais recente aparece primeiro.
    """
    arquivos = glob.glob(padrao)
    arquivos.sort(key=_chave_recencia, reverse=True)
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
    arq_master = arquivo_mais_recente(padrao_master)
    if not arq_master:
        return pd.DataFrame()
    df = pd.read_csv(arq_master, sep=";", encoding="utf-8-sig")
    # corrigir eventuais problemas de codificação nos nomes de coluna
    rename_map = {
        'Avalia��es Amazon': 'Avaliações Amazon',
        'Avalia��es ML': 'Avaliações ML',
        'Pre�o Prod Amazon': 'Preço Prod Amazon',
        'Pre�o Prod ML': 'Preço Prod ML'
    }
    df = df.rename(columns=rename_map)

    # obtém os últimos cinco arquivos "sample" de cada plataforma
    amz_sample = arquivos_ultimos_n("data/resultados_amazon/mais_vendidos_amazon_SAMPLE_*.csv", 5)
    ml_sample = arquivos_ultimos_n("data/resultados_ml/mais_vendidos_ml_SAMPLE_*.csv", 5)

    amz_dict = precos_por_nome(amz_sample, nome_col="Nome", preco_col="Preço")
    # no ML o campo chama-se "Preço à vista" (observado nos samples)
    ml_dict = precos_por_nome(ml_sample, nome_col="Nome", preco_col="Preço à vista")

    df["Produto Amazon"] = df["Produto Amazon"].astype(str)
    df["Produto Mercado Livre"] = df["Produto Mercado Livre"].astype(str)
    # preços já existem, mas vamos preencher apenas quando estiverem vazios
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
