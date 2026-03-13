from flask import Flask, render_template, request
import pandas as pd
import re
import importlib
import os
import time
from functools import lru_cache

from utils.loaders import (
    carregar_comparacao_master_com_precos,
    carregar_amazon_sample,
    carregar_ml_sample,
    carregar_ultimos_amazon_full,
    carregar_ultimos_ml_full,
    atualizar_cache_comentarios,
    buscar_comentarios_cache_por_produto,
)

app = Flask(__name__)

# load once on startup
DF_COMP = carregar_comparacao_master_com_precos()
DF_AMZ = carregar_amazon_sample()
DF_ML = carregar_ml_sample()

CACHE_UPDATE_INTERVAL_SECONDS = 20 * 60
SUMMARY_PIPELINE_VERSION = "v3"

# helper functions from streamlit app

def norm_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def norm_asin(x):
    return norm_str(x).upper()

def limpar_categoria(cat: str) -> str:
    c = norm_str(cat)
    if not c or c.lower() == "nan":
        return ""
    if len(c) < 3:
        return ""
    return c


def parse_price_value(p):
    """Converte uma string de preço para float (ou retorna None)."""
    if pd.isna(p):
        return None
    s = str(p).strip()
    if not s or s.lower().startswith("não"):
        return None
    s = s.replace("R$", "").replace("$", "").replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _extrair_textos_comentarios(comentarios_plataforma: list[dict], limite: int = 40) -> list[str]:
    """Formata os comentarios em strings curtas para indexacao/consulta."""
    textos = []
    for c in comentarios_plataforma[:limite]:
        texto = norm_str(c.get("texto", ""))
        if not texto:
            continue
        nota = norm_str(c.get("nota", ""))
        data = norm_str(c.get("data", ""))
        prefixo = []
        if nota:
            prefixo.append(f"nota={nota}")
        if data:
            prefixo.append(f"data={data}")
        meta = f"[{', '.join(prefixo)}] " if prefixo else ""
        textos.append(f"{meta}{texto}")
    return textos


def _parse_nota(nota_raw) -> float | None:
    s = norm_str(nota_raw).replace(",", ".")
    if not s:
        return None
    try:
        n = float(s)
    except Exception:
        return None
    if n < 0:
        return None
    if n > 5:
        n = 5.0
    return n


def _normalizar_texto(texto: str) -> str:
    return re.sub(r"\s+", " ", norm_str(texto).lower()).strip()


def _analisar_comentarios_plataforma(comentarios_plataforma: list[dict], plataforma: str) -> dict:
    if not comentarios_plataforma:
        return {
            "plataforma": plataforma,
            "qtd": 0,
            "media_nota": None,
            "tom": {"positivo": 0, "neutro": 0, "negativo": 0},
            "temas_pos": [],
            "temas_neg": [],
            "resumo": f"Sem comentarios suficientes na {plataforma}.",
            "score_elogios": 0.0,
        }

    palavras_pos = {
        "bom", "boa", "otimo", "otima", "excelente", "amei", "perfeito", "perfeita", "recomendo",
        "hidrata", "hidratante", "macia", "chegou", "rapido", "qualidade", "vale", "rende",
    }
    palavras_neg = {
        "ruim", "pessimo", "decepcion", "vazou", "vazamento", "aberto", "violado", "falso",
        "imitacao", "atraso", "demorou", "caro", "problema", "defeito", "ressecado", "fraco",
    }

    temas = {
        "hidratacao": ["hidrata", "hidratacao", "ressec", "macia", "pele"],
        "textura": ["textura", "consistencia", "espalha", "denso", "leve"],
        "custo-beneficio": ["preco", "valor", "vale", "promocao", "rende"],
        "entrega": ["entrega", "chegou", "rapido", "atraso", "prazo"],
        "embalagem/lacre": ["embalagem", "caixa", "lacre", "violado", "aberto"],
        "originalidade": ["original", "falso", "imitacao", "autentico"],
    }

    tom = {"positivo": 0, "neutro": 0, "negativo": 0}
    temas_pos = {k: 0 for k in temas}
    temas_neg = {k: 0 for k in temas}
    notas = []

    for c in comentarios_plataforma:
        texto = _normalizar_texto(c.get("texto", ""))
        if not texto:
            continue

        n = _parse_nota(c.get("nota"))
        if n is not None:
            notas.append(n)

        score_pos = sum(1 for p in palavras_pos if p in texto)
        score_neg = sum(1 for p in palavras_neg if p in texto)
        if score_pos > score_neg:
            classe = "positivo"
        elif score_neg > score_pos:
            classe = "negativo"
        else:
            classe = "neutro"
        tom[classe] += 1

        for tema, kws in temas.items():
            if any(k in texto for k in kws):
                if classe == "negativo":
                    temas_neg[tema] += 1
                elif classe == "positivo":
                    temas_pos[tema] += 1

    qtd = len(comentarios_plataforma)
    media_nota = round(sum(notas) / len(notas), 2) if notas else None
    temas_pos_top = [k for k, v in sorted(temas_pos.items(), key=lambda x: x[1], reverse=True) if v > 0][:3]
    temas_neg_top = [k for k, v in sorted(temas_neg.items(), key=lambda x: x[1], reverse=True) if v > 0][:2]

    predominio = "equilibrado"
    if tom["positivo"] > tom["negativo"]:
        predominio = "majoritariamente positivo"
    elif tom["negativo"] > tom["positivo"]:
        predominio = "majoritariamente negativo"

    partes = [
        f"Foram analisados {qtd} comentarios da {plataforma}.",
        f"O tom geral e {predominio} ({tom['positivo']} positivos, {tom['neutro']} neutros e {tom['negativo']} negativos).",
    ]
    if media_nota is not None:
        partes.append(f"A nota media observada foi {media_nota}/5.")
    if temas_pos_top:
        partes.append(f"Elogios mais recorrentes: {', '.join(temas_pos_top)}.")
    if temas_neg_top:
        partes.append(f"Pontos de atencao: {', '.join(temas_neg_top)}.")

    score_elogios = (tom["positivo"] / max(qtd, 1)) * 0.6 + ((media_nota or 3.0) / 5.0) * 0.4

    return {
        "plataforma": plataforma,
        "qtd": qtd,
        "media_nota": media_nota,
        "tom": tom,
        "temas_pos": temas_pos_top,
        "temas_neg": temas_neg_top,
        "resumo": " ".join(partes),
        "score_elogios": round(score_elogios, 4),
    }


def _resumo_ia_aceitavel(texto: str) -> bool:
    t = _normalizar_texto(texto)
    if len(t) < 50:
        return False
    sinais_ruins = [
        "plataforma de e-commerce",
        "site de e-commerce",
        "produto de alta qualidade",
        "no caso especifico",
        "e importante",
        "alternativas se necessario",
        "mercado livre tem uma tendencia",
        "contribuindo para a reputacao",
    ]
    if any(s in t for s in sinais_ruins):
        return False
    padroes_ruins = [
        r"plataforma\s+(amazon|mercado livre)\s+e",
        r"plataforma\s+(amazon|mercado livre)",
        r"e conhecida por oferecer",
        r"o site tem",
        r"sistema de pagamento",
    ]
    if any(re.search(p, t) for p in padroes_ruins):
        return False
    if t.count("alta qualidade") >= 2:
        return False
    return True


def _limpar_saida_llm(texto: str) -> str:
    s = norm_str(texto)
    if not s:
        return s
    # Remove blocos que viram lista/explicacao fora do resumo
    s = re.split(r"\n\s*\n", s)[0]
    s = re.split(r"\n\s*\d+\.", s)[0]
    s = re.split(r"aqui estao", s, flags=re.IGNORECASE)[0]
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _deduplicar_frases(texto: str, max_frases: int = 4) -> str:
    """Remove repeticao de frases quase iguais no resumo gerado."""
    s = norm_str(texto)
    if not s:
        return s
    # Divide por fim de frase mantendo pontuacao basica.
    partes = re.split(r"(?<=[\.!?])\s+", s)
    unicas = []
    vistas = set()
    for p in partes:
        frase = norm_str(p)
        if not frase:
            continue
        chave = re.sub(r"[^a-z0-9 ]", "", frase.lower())
        chave = re.sub(r"\s+", " ", chave).strip()
        if not chave or chave in vistas:
            continue
        vistas.add(chave)
        unicas.append(frase)
        if len(unicas) >= max_frases:
            break
    return " ".join(unicas) if unicas else s


def _resumo_natural_por_fatos(analise: dict) -> str:
    tom = analise.get("tom", {})
    pos = tom.get("positivo", 0)
    neg = tom.get("negativo", 0)
    neut = tom.get("neutro", 0)
    temas_pos = analise.get("temas_pos", [])
    temas_neg = analise.get("temas_neg", [])

    if pos > neg:
        abertura = "Os comentarios mostram percepcao geralmente positiva sobre o produto."
    elif neg > pos:
        abertura = "Os comentarios mostram percepcao mais critica sobre o produto."
    else:
        abertura = "Os comentarios mostram percepcao equilibrada sobre o produto."

    meio = ""
    if temas_pos:
        meio = f"Os elogios mais frequentes destacam {', '.join(temas_pos)}."
    fim = ""
    if temas_neg:
        fim = f"Como ponto de atencao, aparecem relatos ligados a {', '.join(temas_neg)}."

    extra = f"No conjunto, ha {pos} relatos positivos, {neut} neutros e {neg} negativos."
    return " ".join(p for p in [abertura, meio, fim, extra] if p)


def _limpar_meta_texto(texto: str) -> str:
    return re.sub(r"^\[[^\]]*\]\s*", "", norm_str(texto))


def _atualizar_cache_comentarios_se_necessario() -> None:
    """Evita recarregar cache a cada request de detalhe (reduz travamentos)."""
    for plataforma in ("amazon", "ml"):
        cache_path = os.path.join("data", "cache", f"comments_{plataforma}.csv")
        deve_atualizar = True
        if os.path.exists(cache_path):
            try:
                idade = time.time() - os.path.getmtime(cache_path)
                deve_atualizar = idade >= CACHE_UPDATE_INTERVAL_SECONDS
            except Exception:
                deve_atualizar = True
        if deve_atualizar:
            atualizar_cache_comentarios(plataforma, 5)


@lru_cache(maxsize=1)
def _get_modelo_ia_resumo():
    """Carrega pipeline e prompt uma unica vez por processo."""
    try:
        HuggingFacePipeline = importlib.import_module("langchain_huggingface").HuggingFacePipeline
        PromptTemplate = importlib.import_module("langchain_core.prompts").PromptTemplate
        pipeline = importlib.import_module("transformers").pipeline
    except Exception as exc:
        raise RuntimeError(f"Dependencias de IA indisponiveis: {exc}")

    hf_pipe = pipeline(
        "text-generation",
        model="Qwen/Qwen2.5-0.5B-Instruct",
        max_new_tokens=140,
        do_sample=False,
        repetition_penalty=1.1,
    )
    llm = HuggingFacePipeline(pipeline=hf_pipe)

    prompt = PromptTemplate(
        template=(
            "Voce e um analista de opinioes de produtos. Responda em portugues do Brasil com base apenas nos comentarios fornecidos.\n"
            "Nao invente fatos, nao escreva genericamente sobre marketplace e nao copie frases inteiras dos comentarios.\n"
            "Nao descreva loja/plataforma; foque apenas no que os consumidores dizem sobre o produto.\n"
            "Escreva 1 paragrafo curto, natural, no estilo 'Opinioes em destaque', destacando percepcao geral, elogios recorrentes e eventuais pontos de atencao.\n\n"
            "Comentarios:\n{contexto}\n\n"
            "Resumo:"
        ),
        input_variables=["contexto"],
    )
    return llm, prompt


@lru_cache(maxsize=128)
def _gerar_resumo_rag_cached(assinatura_comentarios: str, textos_amz: tuple[str, ...], textos_ml: tuple[str, ...]) -> dict:
    """Gera resumo de IA com cache por assinatura dos comentarios."""
    del assinatura_comentarios  # usado apenas como chave de cache

    llm, prompt = _get_modelo_ia_resumo()

    analise_amz = _analisar_comentarios_plataforma(
        [{"texto": _limpar_meta_texto(t), "nota": "", "data": ""} for t in textos_amz],
        "Amazon",
    )
    analise_ml = _analisar_comentarios_plataforma(
        [{"texto": _limpar_meta_texto(t), "nota": "", "data": ""} for t in textos_ml],
        "Mercado Livre",
    )

    def _contexto_resumo(textos: tuple[str, ...], analise: dict, limite: int = 8, max_chars: int = 2500) -> str:
        itens = []
        total = 0
        indicadores = [
            f"- tom_geral: {analise.get('resumo', '')}",
        ]
        base = "\n".join(indicadores) + "\n"
        total += len(base)
        for t in textos[:limite]:
            limpo = _limpar_meta_texto(t)
            if not limpo:
                continue
            if len(limpo) > 220:
                limpo = limpo[:220].rstrip() + "..."
            pedaco = f"- {limpo}\n"
            if total + len(pedaco) > max_chars:
                break
            itens.append(pedaco)
            total += len(pedaco)
        return base + "".join(itens)

    def _ask(textos: tuple[str, ...], analise: dict) -> str:
        contexto = _contexto_resumo(textos, analise)
        if not contexto:
            return ""
        final_prompt = prompt.format(contexto=contexto)
        result = llm.invoke(final_prompt)
        raw = result if isinstance(result, str) else str(result)
        texto = raw.strip()
        if texto.startswith(final_prompt):
            texto = texto[len(final_prompt):].strip()
        if "Resumo:" in texto:
            texto = texto.split("Resumo:")[-1].strip()
        return _limpar_saida_llm(texto)

    def _ask_rewrite_from_facts(analise: dict) -> str:
        fatos = []
        fatos.append(f"tom: {analise.get('tom', {})}")
        if analise.get("temas_pos"):
            fatos.append(f"elogios recorrentes: {', '.join(analise['temas_pos'])}")
        if analise.get("temas_neg"):
            fatos.append(f"pontos de atencao: {', '.join(analise['temas_neg'])}")
        if analise.get("media_nota") is not None:
            fatos.append(f"nota media aproximada: {analise['media_nota']}")

        prompt_fatos = (
            "Voce vai escrever um resumo curto de IA em portugues do Brasil, estilo 'Opinioes em destaque'.\n"
            "Foque apenas no produto e no que os consumidores relatam, sem explicar loja/plataforma.\n"
            "Nao use frases como 'foram analisados' e nao use tom tecnico.\n"
            "Entregue 1 paragrafo natural (3 a 5 frases).\n\n"
            f"Fatos extraidos: {'; '.join(fatos)}\n\n"
            "Resumo:"
        )
        result = llm.invoke(prompt_fatos)
        raw = result if isinstance(result, str) else str(result)
        texto = raw.strip()
        if texto.startswith(prompt_fatos):
            texto = texto[len(prompt_fatos):].strip()
        if "Resumo:" in texto:
            texto = texto.split("Resumo:")[-1].strip()
        return _limpar_saida_llm(texto)

    resumo_amz = "Sem comentarios suficientes na Amazon."
    if textos_amz:
        resumo_amz = _ask(textos_amz, analise_amz)
        resumo_amz = re.sub(r"\[[^\]]*\]", "", resumo_amz)
        resumo_amz = _deduplicar_frases(resumo_amz)
        if not _resumo_ia_aceitavel(resumo_amz):
            resumo_amz = _ask_rewrite_from_facts(analise_amz)
            resumo_amz = re.sub(r"\[[^\]]*\]", "", resumo_amz)
            resumo_amz = _deduplicar_frases(resumo_amz)
        if not _resumo_ia_aceitavel(resumo_amz):
            resumo_amz = _resumo_natural_por_fatos(analise_amz)
            resumo_amz = _deduplicar_frases(resumo_amz)

    resumo_ml = "Sem comentarios suficientes no Mercado Livre."
    if textos_ml:
        resumo_ml = _ask(textos_ml, analise_ml)
        resumo_ml = re.sub(r"\[[^\]]*\]", "", resumo_ml)
        resumo_ml = _deduplicar_frases(resumo_ml)
        if not _resumo_ia_aceitavel(resumo_ml):
            resumo_ml = _ask_rewrite_from_facts(analise_ml)
            resumo_ml = re.sub(r"\[[^\]]*\]", "", resumo_ml)
            resumo_ml = _deduplicar_frases(resumo_ml)
        if not _resumo_ia_aceitavel(resumo_ml):
            resumo_ml = _resumo_natural_por_fatos(analise_ml)
            resumo_ml = _deduplicar_frases(resumo_ml)

    return {
        "amazon": resumo_amz,
        "ml": resumo_ml,
        "modo": "rag",
    }


def gerar_resumo_comentarios(comentarios: dict) -> dict:
    """Orquestra resumo de comentarios com RAG (LangChain/HF), sem fallback generico."""
    textos_amz = tuple(_extrair_textos_comentarios(comentarios.get("amazon", [])))
    textos_ml = tuple(_extrair_textos_comentarios(comentarios.get("ml", [])))

    if not textos_amz and not textos_ml:
        return {
            "amazon": "Sem comentarios da Amazon para resumir.",
            "ml": "Sem comentarios do Mercado Livre para resumir.",
            "modo": "sem_dados",
            "erro": "",
        }

    assinatura = str(hash((SUMMARY_PIPELINE_VERSION, textos_amz, textos_ml)))
    try:
        out = _gerar_resumo_rag_cached(assinatura, textos_amz, textos_ml)
        out["erro"] = ""
        return out
    except Exception as exc:
        return {
            "amazon": "Resumo de IA indisponivel no momento para Amazon.",
            "ml": "Resumo de IA indisponivel no momento para Mercado Livre.",
            "modo": "ia_indisponivel",
            "erro": str(exc),
        }


# helper to determine which platform currently offers the best price
# accepts two raw price strings (as they appear in the dataframe)
def melhor_plataforma(preco_amazon: str | None, preco_ml: str | None) -> str:
    pa = parse_price_value(preco_amazon)
    pm = parse_price_value(preco_ml)
    if pa is None and pm is None:
        return "Nenhum"
    if pa is None:
        return "Mercado Livre"
    if pm is None:
        return "Amazon"
    if pa < pm:
        return "Amazon"
    if pm < pa:
        return "Mercado Livre"
    return "Empate"


@app.template_filter('to_stars')
def to_stars(value):
    """Converte valores de nota em inteiro de 0 a 5 para renderizar estrelas.

    Aceita strings como '5,0', '4.5' ou números. Retorna inteiro entre 0 e 5.
    """
    if value is None:
        return 0
    try:
        s = str(value).strip()
        s = s.replace(',', '.')
        f = float(s)
    except Exception:
        return 0
    i = int(round(f))
    if i < 0:
        i = 0
    if i > 5:
        i = 5
    return i

# lookup prices from samples (fallback)
DF_AMZ["ASIN"] = DF_AMZ["ASIN"].apply(norm_asin)
DF_ML["ASIN"] = DF_ML["ASIN"].apply(norm_asin)

AMZ_PRICE = (
    DF_AMZ.dropna(subset=["ASIN"])
          .drop_duplicates(subset=["ASIN"], keep="last")
          .set_index("ASIN")["Preço"]
          .astype(str)
          .to_dict()
)

ML_PRICE = (
    DF_ML.dropna(subset=["ASIN"])
         .drop_duplicates(subset=["ASIN"], keep="last")
         .set_index("ASIN")["Preço à vista"]
         .astype(str)
         .to_dict()
)

ML_PRICE_LINK = {}
if "Link" in DF_ML.columns:
    tmp = DF_ML.assign(Link=DF_ML["Link"].astype(str).str.strip())
    ML_PRICE_LINK = (
        tmp[tmp["Link"] != ""]
           .drop_duplicates(subset=["Link"], keep="last")
           .set_index("Link")["Preço à vista"]
           .astype(str)
           .to_dict()
    )


def buscar_preco_amazon(asin):
    a = norm_asin(asin)
    if a:
        # procurar nos últimos FULLs por ASIN (mais recentes primeiro)
        try:
            amz_fulls = carregar_ultimos_amazon_full(5)
            for df, ctime, path in amz_fulls:
                if 'ASIN' in df.columns:
                    df_asin = df[df['ASIN'].apply(lambda x: norm_asin(x) == a)]
                    if not df_asin.empty:
                        # pegar o preço da primeira ocorrência (arquivo mais recente)
                        val = df_asin.iloc[0].get('Preço', '')
                        if val and str(val).strip().lower() != 'nan':
                            return str(val)
        except Exception:
            pass
    return AMZ_PRICE.get(a, "Não disponível")


def buscar_preco_ml(asin, link):
    asin = norm_asin(asin)
    if asin and asin.lower() != "nan":
        # procurar nos últimos FULLs por ASIN (mais recentes primeiro)
        try:
            ml_fulls = carregar_ultimos_ml_full(5)
            for df, ctime, path in ml_fulls:
                if 'ASIN' in df.columns:
                    df_asin = df[df['ASIN'].apply(lambda x: norm_asin(x) == asin)]
                    if not df_asin.empty:
                        val = df_asin.iloc[0].get('Preço à vista', '') or df_asin.iloc[0].get('Preço', '')
                        if val and str(val).strip().lower() != 'nan':
                            return str(val)
        except Exception:
            pass
        p = ML_PRICE.get(asin)
        if p and str(p).lower() != "nan":
            return p
    link = norm_str(link)
    if link:
        p = ML_PRICE_LINK.get(link)
        if p and str(p).lower() != "nan":
            return p
    return "Não disponível"

def buscar_comentarios_produto(nome_amazon: str, nome_ml: str | None = None):
    """Recupera avaliações de Amazon e Mercado Livre para um par de nomes.

    A comparação é feita diretamente contra a coluna ``Nome`` dos arquivos
    *FULL* de cada plataforma. Passar ``nome_ml`` garante que usamos o
    valor da coluna **Produto Mercado Livre** da base de comparação (que pode
    ser diferente do nome Amazon).

    Retorna um dicionário com duas listas sob as chaves ``amazon`` e ``ml``.
    Cada item é um dict contendo ``texto``, ``nota`` e ``data``.
    """
    comentarios = {"amazon": [], "ml": []}

    # --------------------------------------------------
    # Amazon -- procurar nos últimos N FULLs (mais recentes primeiro)
    if nome_amazon:
        nome_norm = norm_str(nome_amazon).upper()
        amz_fulls = carregar_ultimos_amazon_full(5)
        seen_texts = set()
        for df, ctime, path in amz_fulls:
            if 'Nome' not in df.columns:
                continue
            df_prod = df[df['Nome'].apply(lambda x: norm_str(x).upper() == nome_norm)]
            for _, row in df_prod.iterrows():
                comentario = norm_str(row.get('Comentário', ''))
                if not comentario:
                    continue
                # evitar duplicatas de texto, preferindo os mais recentes
                if comentario in seen_texts:
                    continue
                seen_texts.add(comentario)
                comentarios['amazon'].append({
                    'texto': comentario,
                    'nota': row.get('Nota Comentário', ''),
                    'data': row.get('Data Comentário', '')
                })

    # --------------------------------------------------
    # Mercado Livre
    if nome_ml is None:
        # se não passou nome_ml, tente reaproveitar o mesmo nome Amazon
        nome_ml = nome_amazon

    if nome_ml:
        nome_norm_ml = norm_str(nome_ml).upper()
        ml_fulls = carregar_ultimos_ml_full(5)
        seen_texts_ml = set()
        for df, ctime, path in ml_fulls:
            if 'Nome' not in df.columns:
                continue
            df_prod = df[df['Nome'].apply(lambda x: norm_str(x).upper() == nome_norm_ml)]
            for _, row in df_prod.iterrows():
                comentario = norm_str(row.get('Comentário', ''))
                if not comentario:
                    continue
                if comentario in seen_texts_ml:
                    continue
                seen_texts_ml.add(comentario)
                comentarios['ml'].append({
                    'texto': comentario,
                    'nota': row.get('Nota Comentário', ''),
                    'data': row.get('Data Comentário', '')
                })

    return comentarios

@app.route("/", methods=["GET"])
def home():
    return render_template("home.html")

@app.route("/buscar", methods=["GET"])
def buscar():
    # categories from amazon sample
    df = DF_AMZ.copy()
    df["Subcategoria"] = df["Subcategoria"].apply(limpar_categoria)
    df = df[df["Subcategoria"] != ""]
    categorias = ["Todas"] + sorted(df["Subcategoria"].unique().tolist())

    categoria = request.args.get("categoria", "Todas")
    produto = request.args.get("produto", "")

    df_filtrado = DF_COMP.copy()
    df_filtrado["ASIN Amazon"] = df_filtrado["ASIN Amazon"].apply(norm_asin)

    if categoria != "Todas":
        asins_cat = df[df["Subcategoria"] == categoria]["ASIN"].unique()
        df_filtrado = df_filtrado[df_filtrado["ASIN Amazon"].isin(asins_cat)]

    if produto:
        df_filtrado = df_filtrado[df_filtrado["Produto Amazon"] == produto]

    produtos = sorted([p for p in df_filtrado["Produto Amazon"].unique().tolist() if p])

    # attach prices if missing
    df_filtrado["Preço Prod Amazon"] = df_filtrado.apply(
        lambda r: buscar_preco_amazon(r["ASIN Amazon"]) if not r.get("Preço Prod Amazon") else r.get("Preço Prod Amazon"),
        axis=1
    )
    df_filtrado["Preço Prod ML"] = df_filtrado.apply(
        lambda r: buscar_preco_ml(r.get("ASIN Mercado Livre", ""), r.get("Link Mercado Livre", ""))
        if not r.get("Preço Prod ML") else r.get("Preço Prod ML"),
        axis=1
    )

    # compute which platform has better price (reuse shared helper)
    df_filtrado["Melhor Preço"] = df_filtrado.apply(
        lambda r: melhor_plataforma(r.get("Preço Prod Amazon"), r.get("Preço Prod ML")),
        axis=1
    )

    return render_template(
        "index.html",
        categorias=categorias,
        produtos=produtos,
        escolha_categoria=categoria,
        escolha_produto=produto,
        resultados=df_filtrado.to_dict(orient="records"),
    )

@app.route("/produto/<produto_nome>", methods=["GET"])
def detalhe_produto(produto_nome):
    """Página de detalhe do produto com comentários"""
    # Encontrar o produto na comparação
    df_comp_copy = DF_COMP.copy()
    df_comp_copy["Produto Amazon"] = df_comp_copy["Produto Amazon"].astype(str)
    produto = df_comp_copy[df_comp_copy["Produto Amazon"] == produto_nome]
    
    if produto.empty:
        return render_template("erro.html", mensagem="Produto não encontrado"), 404
    
    produto_info = produto.iloc[0].to_dict()
    
    # Atualiza cache somente quando estiver vencido (evita lentidao e timeout)
    _atualizar_cache_comentarios_se_necessario()

    nome_amazon = produto_info.get("Produto Amazon", "")
    nome_ml = produto_info.get("Produto Mercado Livre", "")
    comentarios = {
        'amazon': buscar_comentarios_cache_por_produto('amazon', nome_amazon),
        'ml': buscar_comentarios_cache_por_produto('ml', nome_ml if nome_ml else nome_amazon),
    }
    resumo_comentarios = gerar_resumo_comentarios(comentarios)
    
    # Buscar preços se não existem
    if not produto_info.get("Preço Prod Amazon"):
        produto_info["Preço Prod Amazon"] = buscar_preco_amazon(produto_info.get("ASIN Amazon", ""))
    if not produto_info.get("Preço Prod ML"):
        produto_info["Preço Prod ML"] = buscar_preco_ml(
            produto_info.get("ASIN Mercado Livre", ""),
            produto_info.get("Link Mercado Livre", "")
        )

    # compute and store best-price indicator for template reuse
    produto_info["Melhor Preço"] = melhor_plataforma(
        produto_info.get("Preço Prod Amazon"),
        produto_info.get("Preço Prod ML"),
    )

    # Marcar classes para estilizar o preço mais barato (+verde) e mais caro (+amarelo)
    produto_info["amazon_price_class"] = ""
    produto_info["ml_price_class"] = ""
    pa = parse_price_value(produto_info.get("Preço Prod Amazon"))
    pm = parse_price_value(produto_info.get("Preço Prod ML"))
    if pa is None and pm is None:
        pass
    elif pa is None:
        produto_info["ml_price_class"] = "price-cheap"
    elif pm is None:
        produto_info["amazon_price_class"] = "price-cheap"
    else:
        if pa < pm:
            produto_info["amazon_price_class"] = "price-cheap"
            produto_info["ml_price_class"] = "price-expensive"
        elif pm < pa:
            produto_info["ml_price_class"] = "price-cheap"
            produto_info["amazon_price_class"] = "price-expensive"
    
    return render_template(
        "produto_detail.html",
        produto=produto_info,
        comentarios=comentarios,
        resumo_comentarios=resumo_comentarios,
    )

if __name__ == "__main__":
    # Watchdog reload with heavy ML deps (torch/transformers) can restart the
    # dev server during requests and trigger connection reset errors.
    app.run(debug=False, use_reloader=False, threaded=True)
