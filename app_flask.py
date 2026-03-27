from flask import Flask, render_template, request
import pandas as pd
import re
import importlib
import os
import time
import requests
from urllib.parse import quote
from functools import lru_cache
from huggingface_hub import InferenceClient

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
@app.route("/healthz")
def healthz():
    return "ok", 200

# load once on startup
DF_COMP = pd.DataFrame()
DF_AMZ = None
DF_ML = None
DF_COMP_LAST_REFRESH = 0

USE_LLM_POLISH = os.getenv("USE_LLM_POLISH", "0") == "1"
CACHE_UPDATE_INTERVAL_SECONDS = 20 * 60
DF_COMP_REFRESH_INTERVAL_SECONDS = int(os.getenv("DF_COMP_REFRESH_INTERVAL_SECONDS", "120"))
SUMMARY_PIPELINE_VERSION = "v3"
HF_SUMMARY_MODEL = os.getenv("HF_SUMMARY_MODEL", "").strip()
HF_SUMMARY_FALLBACK_MODELS = tuple(
    m.strip()
    for m in os.getenv("HF_SUMMARY_FALLBACK_MODELS", "").split(",")
    if m.strip()
)
HF_INFERENCE_TIMEOUT = int(os.getenv("HF_INFERENCE_TIMEOUT", "60"))
ALLOW_LOCAL_LLM_FALLBACK = os.getenv("ALLOW_LOCAL_LLM_FALLBACK", "0") == "1"

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

def _extrair_termos_relevantes(comentarios_plataforma: list[dict], limite: int = 3) -> list[str]:
    stop = {
        "de", "da", "do", "das", "dos", "a", "o", "as", "os", "e", "em", "na", "no", "nas", "nos",
        "um", "uma", "uns", "umas", "para", "por", "com", "sem", "que", "se", "ao", "aos",
        "mais", "muito", "muita", "produto", "produtos", "bom", "boa", "otimo", "otima",
        "excelente", "amei", "perfeito", "perfeita", "recomendo", "qualidade", "vale", "rende",
        "chegou", "rapido", "hidrata", "hidratante", "macia", "textura", "preco", "valor", "pele",
        "minha", "meu", "minhas", "meus", "dele", "dela", "isso", "essa", "esse", "mim", "pra",
        "porque", "muito", "bem", "super", "gostei", "achei", "usar", "uso", "produto", "compra"
    }

    contagem = {}

    for c in comentarios_plataforma:
        texto = _normalizar_texto(c.get("texto", ""))
        if not texto:
            continue

        tokens = re.findall(r"[a-zà-ÿ0-9-]+", texto)
        for t in tokens:
            t = t.strip().lower()
            if len(t) < 4:
                continue
            if t in stop:
                continue
            if t.isdigit():
                continue
            contagem[t] = contagem.get(t, 0) + 1

    termos = [k for k, v in sorted(contagem.items(), key=lambda x: x[1], reverse=True) if v > 1]
    return termos[:limite]


def _analisar_comentarios_plataforma(comentarios_plataforma: list[dict], plataforma: str) -> dict:
    if not comentarios_plataforma:
        return {
            "plataforma": plataforma,
            "qtd": 0,
            "media_nota": None,
            "tom": {"positivo": 0, "neutro": 0, "negativo": 0},
            "temas_pos": [],
            "temas_neg": [],
            "termos_top": [],
            "resumo": f"Sem comentários suficientes na {plataforma}.",
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
    termos_top = _extrair_termos_relevantes(comentarios_plataforma, limite=3)

    predominio = "equilibrado"
    if tom["positivo"] > tom["negativo"]:
        predominio = "majoritariamente positivo"
    elif tom["negativo"] > tom["positivo"]:
        predominio = "majoritariamente negativo"

    partes = [
        f"Foram analisados {qtd} comentários da {plataforma}.",
        f"O tom geral é {predominio} ({tom['positivo']} positivos, {tom['neutro']} neutros e {tom['negativo']} negativos).",
    ]
    if media_nota is not None:
        partes.append(f"A nota média observada foi {media_nota}/5.")
    if temas_pos_top:
        partes.append(f"Elogios mais recorrentes: {', '.join(temas_pos_top)}.")
    if temas_neg_top:
        partes.append(f"Pontos de atenção: {', '.join(temas_neg_top)}.")
    if termos_top:
        partes.append(f"Termos frequentes: {', '.join(termos_top)}.")

    score_elogios = (tom["positivo"] / max(qtd, 1)) * 0.6 + ((media_nota or 3.0) / 5.0) * 0.4

    return {
        "plataforma": plataforma,
        "qtd": qtd,
        "media_nota": media_nota,
        "tom": tom,
        "temas_pos": temas_pos_top,
        "temas_neg": temas_neg_top,
        "termos_top": termos_top,
        "resumo": " ".join(partes),
        "score_elogios": round(score_elogios, 4),
    }


def _resumo_ia_aceitavel(texto: str) -> bool:
    t = _normalizar_texto(texto)
    if len(t) < 50:
        return False
    sinais_instrucao = [
        "use 1 paragrafo",
        "use 1 parágrafo",
        "escreva 1 paragrafo",
        "escreva 1 parágrafo",
        "foco em percepcao geral",
        "foco em percepção geral",
        "foque apenas no produto",
        "nao mencione plataforma",
        "não mencione plataforma",
        "nao reproduza frases literais",
        "não reproduza frases literais",
        "destaque percepcao geral",
        "destaque percepção geral",
        "nao use frases como",
        "não use frases como",
        "nao use tom tecnico",
        "não use tom técnico",
        "entregue 1 paragrafo",
        "entregue 1 parágrafo",
        "voce vai escrever um resumo",
        "você vai escrever um resumo",
        "resuma as opinioes",
        "resuma as opiniões",
        "resuma as opinioes de consumidores",
        "resuma as opiniões de consumidores",
        "dados estruturados",
        "comentarios:",
        "comentários:",
    ]
    if any(s in t for s in sinais_instrucao):
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
        "bbc brasil",
        "serviço mundial de saúde",
        "organizacao mundial da saude",
        "publicado na ultima",
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


def _resumo_parece_copia_comentario(resumo: str, comentarios: tuple[str, ...]) -> bool:
    """Sinaliza resumo muito extrativo (copia frases dos comentarios)."""
    r = _normalizar_texto(resumo)
    if not r:
        return True

    frases = [
        _normalizar_texto(f)
        for f in re.split(r"(?<=[\.!?])\s+", resumo)
        if _normalizar_texto(f)
    ]
    comentarios_norm = [
        _normalizar_texto(_limpar_meta_texto(c)) for c in comentarios[:12] if _normalizar_texto(_limpar_meta_texto(c))
    ]

    for frase in frases:
        if len(frase) < 40:
            continue
        for c in comentarios_norm:
            # Evita exibir resumo que replica trecho inteiro de comentario.
            if frase in c:
                return True
    return False


def _tokens_base(texto: str) -> set[str]:
    stop = {
        "de", "da", "do", "das", "dos", "a", "o", "as", "os", "e", "em", "na", "no", "nas", "nos",
        "um", "uma", "uns", "umas", "para", "por", "com", "sem", "que", "se", "ao", "aos", "mais",
    }
    toks = re.findall(r"[a-zA-Zà-ÿÀ-Ÿ0-9-]+", _normalizar_texto(texto))
    return {t for t in toks if len(t) > 2 and t not in stop}


def _resumo_aderente_aos_comentarios(resumo: str, comentarios: tuple[str, ...]) -> bool:
    """Garante que o resumo esteja semanticamente próximo das avaliações fornecidas."""
    resumo_tokens = _tokens_base(resumo)
    if len(resumo_tokens) < 6:
        return False

    origem = " ".join(_limpar_meta_texto(c) for c in comentarios[:20])
    origem_tokens = _tokens_base(origem)
    if not origem_tokens:
        return False

    inter = len(resumo_tokens & origem_tokens)
    taxa = inter / max(len(resumo_tokens), 1)
    return taxa >= 0.2


def _formatar_lista_natural(itens: list[str]) -> str:
    if not itens:
        return ""
    if len(itens) == 1:
        return itens[0]
    if len(itens) == 2:
        return f"{itens[0]} e {itens[1]}"
    return ", ".join(itens[:-1]) + f" e {itens[-1]}"


def _tema_para_exibicao(tema: str) -> str:
    mapa = {
        "hidratacao": "hidratação",
        "textura": "textura",
        "custo-beneficio": "custo-benefício",
        "entrega": "entrega",
        "embalagem/lacre": "embalagem e lacre",
        "originalidade": "autenticidade",
    }
    return mapa.get(tema, tema.replace("-", " "))


def _remover_frases_instrucao(texto: str) -> str:
    """Filtra frases que sao instrucoes do prompt, nao resumo do produto."""
    s = norm_str(texto)
    if not s:
        return s

    partes = re.split(r"(?<=[\.!?])\s+", s)
    inicio_bloqueado = (
        "use",
        "escreva",
        "foque",
        "destaque",
        "resuma",
        "nao mencione",
        "nao reproduza",
        "nao use",
        "entregue",
        "voce vai",
    )
    contem_bloqueado = (
        "dados estruturados",
        "fatos consolidados",
        "fatos extraidos",
        "comentarios:",
        "foco em percepcao geral",
    )

    limpas = []
    for p in partes:
        frase = norm_str(p)
        if not frase:
            continue
        f_norm = _normalizar_texto(frase)
        if any(f_norm.startswith(i) for i in inicio_bloqueado):
            continue
        if any(c in f_norm for c in contem_bloqueado):
            continue
        limpas.append(frase)

    return " ".join(limpas).strip()


def _limpar_saida_llm(texto: str) -> str:
    s = norm_str(texto)
    if not s:
        return s
    # Remove blocos que viram lista/explicacao fora do resumo
    s = re.split(r"\n\s*\n", s)[0]
    s = re.split(r"\n\s*\d+\.", s)[0]
    s = re.split(r"aqui estao", s, flags=re.IGNORECASE)[0]
    if "Resumo:" in s:
        s = s.split("Resumo:")[-1].strip()
    s = _remover_frases_instrucao(s)
    s = re.sub(r"\s+", " ", s).strip()
    frases = [f.strip() for f in re.split(r"(?<=[\.!?])\s+", s) if f.strip()]
    if len(frases) > 4:
        s = " ".join(frases[:4]).strip()
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
    qtd = analise.get("qtd", 0)
    tom = analise.get("tom", {})
    pos = tom.get("positivo", 0)
    neg = tom.get("negativo", 0)
    neut = tom.get("neutro", 0)

    temas_pos = [_tema_para_exibicao(t) for t in analise.get("temas_pos", [])[:3]]
    temas_neg = [_tema_para_exibicao(t) for t in analise.get("temas_neg", [])[:2]]
    termos = analise.get("termos_top", [])[:3]

    if qtd == 0:
        return "Ainda não há comentários suficientes para gerar um resumo confiável."

    def lista_natural(lista):
        lista = [str(x).strip() for x in lista if str(x).strip()]
        if not lista:
            return ""
        if len(lista) == 1:
            return lista[0]
        if len(lista) == 2:
            return f"{lista[0]} e {lista[1]}"
        return ", ".join(lista[:-1]) + f" e {lista[-1]}"

    predominio_positivo = pos >= max(neg, neut)
    predominio_negativo = neg > pos and neg >= neut

    # aberturas mais naturais e menos repetitivas
    if predominio_positivo:
        aberturas = [
            "O conjunto de comentários sugere uma percepção bastante positiva sobre o produto.",
            "Pelas avaliações, o produto tende a agradar a maior parte dos consumidores.",
            "As opiniões reunidas passam uma impressão geral favorável sobre a experiência de uso.",
        ]
    elif predominio_negativo:
        aberturas = [
            "As avaliações revelam uma experiência mais irregular do que positiva.",
            "Os comentários mostram que a percepção sobre o produto é mais dividida.",
            "No geral, as opiniões indicam que o produto não atende igualmente bem a todos os consumidores.",
        ]
    else:
        aberturas = [
            "Os comentários mostram uma percepção relativamente equilibrada sobre o produto.",
            "As avaliações apontam uma experiência mista, com elogios e ressalvas aparecendo lado a lado.",
            "O conjunto de opiniões sugere uma recepção moderada, sem consenso absoluto entre os consumidores.",
        ]

    idx = (qtd + pos + neg + neut) % 3
    abertura = aberturas[idx]

    frases = [abertura]

    # elogios
    if temas_pos:
        elogios_modelos = [
            f"Entre os aspectos mais valorizados, aparecem {lista_natural(temas_pos)}.",
            f"Os elogios se concentram principalmente em {lista_natural(temas_pos)}.",
            f"Os comentários positivos costumam destacar {lista_natural(temas_pos)}.",
        ]
        frases.append(elogios_modelos[(pos + qtd) % 3])

    # termos específicos ajudam a diferenciar Amazon e ML
    termos_filtrados = []
    blacklist = {
        "minha", "meu", "produto", "coisa", "muito", "pouco", "bem", "super",
        "pra", "para", "comprei", "gostei", "achei", "usar", "uso"
    }
    for t in termos:
        tt = str(t).strip().lower()
        if tt and tt not in blacklist and len(tt) > 3:
            termos_filtrados.append(tt)

    if termos_filtrados:
        frases.append(
            f"Também surgem com frequência menções a {lista_natural(termos_filtrados[:2])}, o que ajuda a definir o perfil das experiências relatadas."
        )

    # ressalvas
    if temas_neg:
        ressalvas_modelos = [
            f"Ainda assim, há observações pontuais relacionadas a {lista_natural(temas_neg)}.",
            f"Apesar da leitura geral positiva, algumas avaliações mencionam questões ligadas a {lista_natural(temas_neg)}.",
            f"Os pontos de atenção aparecem de forma mais discreta, mas envolvem {lista_natural(temas_neg)}.",
        ]
        frases.append(ressalvas_modelos[(neg + qtd) % 3])

    # fechamento mais editorial
    if predominio_positivo and not temas_neg:
        fechamentos = [
            "No fim, a percepção predominante é de que se trata de uma compra que costuma atender bem às expectativas.",
            "De forma geral, o produto é visto como uma opção satisfatória dentro da proposta que oferece.",
            "No conjunto, a experiência relatada tende a ser positiva e coerente com o que os consumidores esperam.",
        ]
    elif predominio_positivo and temas_neg:
        fechamentos = [
            "No geral, a experiência relatada é positiva, embora com ajustes pontuais de expectativa em alguns casos.",
            "Em síntese, o produto agrada na maior parte dos relatos, mesmo com ressalvas específicas.",
            "No balanço das opiniões, os elogios prevalecem, ainda que existam observações pontuais.",
        ]
    elif predominio_negativo:
        fechamentos = [
            "No conjunto, a impressão final é mais cautelosa do que entusiasmada.",
            "Assim, a percepção geral acaba sendo menos consistente do que os comentários positivos isolados sugerem.",
            "Em resumo, as opiniões não convergem para uma experiência amplamente satisfatória.",
        ]
    else:
        fechamentos = [
            "No fim, a percepção geral depende bastante do tipo de expectativa que o consumidor leva para o uso.",
            "Em resumo, trata-se de um produto que divide opiniões em pontos específicos, embora mantenha avaliações razoáveis no conjunto.",
            "No balanço final, os relatos mostram uma experiência válida, mas não totalmente uniforme entre os consumidores.",
        ]

    frases.append(fechamentos[(qtd + pos) % 3])

    texto = " ".join(frases)

    # limpeza final para evitar repetições incômodas
    texto = texto.replace("  ", " ").strip()
    return texto

def _resumo_via_hf_inference_api(comentarios: tuple[str, ...], analise: dict) -> str:
    print("[HF] entrou em _resumo_via_hf_inference_api")
    print("[HF] qtd comentarios:", len(comentarios))
    
    if not comentarios:
        return ""

    trechos = []
    total_chars = 0
    max_chars = 1800

    for c in comentarios[:8]:
        limpo = _limpar_meta_texto(c)
        if not limpo:
            continue
        limpo = re.sub(r"\s+", " ", limpo).strip()
        if len(limpo) > 220:
            limpo = limpo[:220].rstrip() + "..."
        bloco = f"- {limpo}"
        if total_chars + len(bloco) > max_chars:
            break
        trechos.append(bloco)
        total_chars += len(bloco)

    if not trechos:
        return ""

    contexto = "\n".join(trechos)

    prompt = (
        "Resuma as opinioes abaixo sobre um produto em um unico paragrafo curto, natural e fluido, "
        "em portugues do Brasil. O texto deve soar como um resumo editorial de experiencia de consumidores. "
        "Destaque a percepcao geral, os elogios mais recorrentes e eventuais ressalvas de forma sutil. "
        "Nao mencione plataforma, quantidade de comentarios, nota media ou percentuais. "
        "Nao use listas. Nao invente fatos.\n\n"
        f"{contexto}\n\nResumo:"
    )

    texto = _hf_request_json({
        "inputs": prompt,
        "parameters": {
            "max_new_tokens": 140,
            "temperature": 0.7,
        },
    })
    print("[HF] resumo retornado:", texto[:300] if texto else "VAZIO")
    return norm_str(texto)


def _hf_generate_prompt(prompt: str, max_new_tokens: int = 160, temperature: float = 0.2) -> str:
    payload = {
        "inputs": prompt,
        "parameters": {
            "max_new_tokens": max_new_tokens,
            "temperature": temperature,
        },
    }
    data = _hf_request_json(payload)
    return _extrair_texto_hf(data)


def _extrair_texto_hf(data) -> str:
    """Normaliza diferentes formatos de resposta da HF Inference API."""
    texto = ""

    if isinstance(data, list) and data:
        primeiro = data[0]
        if isinstance(primeiro, dict):
            texto = (
                primeiro.get("generated_text")
                or primeiro.get("summary_text")
                or primeiro.get("text")
                or ""
            )
        elif isinstance(primeiro, str):
            texto = primeiro
    elif isinstance(data, dict):
        texto = (
            data.get("generated_text")
            or data.get("summary_text")
            or data.get("text")
            or ""
        )
        if not texto and isinstance(data.get("choices"), list) and data["choices"]:
            c0 = data["choices"][0]
            if isinstance(c0, dict):
                texto = c0.get("text") or c0.get("message", {}).get("content", "")

    return _limpar_saida_llm(texto)


def _hf_request_json(payload: dict) -> str:
    token = os.getenv("HF_API_TOKEN", "").strip()
    print("[HF] token existe?", bool(token))
    print("[HF] modelo principal:", HF_SUMMARY_MODEL)
    print("[HF] fallbacks:", HF_SUMMARY_FALLBACK_MODELS)

    if not token:
        raise RuntimeError("HF_API_TOKEN nao configurado")

    modelos = []
    vistos = set()
    for model in (HF_SUMMARY_MODEL, *HF_SUMMARY_FALLBACK_MODELS):
        m = (model or "").strip()
        if m and m not in vistos:
            vistos.add(m)
            modelos.append(m)

    print("[HF] modelos finais:", modelos)

    if not modelos:
        raise RuntimeError("Nenhum modelo configurado para resumo de IA")

    prompt = payload.get("inputs", "")
    params = payload.get("parameters", {}) or {}

    erros = []

    for model in modelos:
        try:
            print(f"[HF] tentando modelo: {model}")
            client = InferenceClient(
                provider="hf-inference",
                api_key=token,
            )

            out = client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "Voce resume opinioes reais de consumidores sobre produtos. "
                            "Escreva um unico paragrafo curto, natural e fluido, em portugues do Brasil. "
                            "Nao invente fatos, nao use listas, nao mencione plataforma, nota media, "
                            "quantidade de comentarios ou percentuais."
                        ),
                    },
                    {
                        "role": "user",
                        "content": prompt,
                    },
                ],
                max_tokens=int(params.get("max_new_tokens", 140)),
                temperature=float(params.get("temperature", 0.7)),
            )

            print("[HF] resposta bruta:", out)

            texto = out.choices[0].message.content if out and out.choices else ""
            texto = norm_str(texto)
            print("[HF] texto final:", texto[:300] if texto else "VAZIO")

            if texto:
                return texto

            erros.append(f"{model}: resposta vazia")

        except Exception as exc:
            print(f"[HF] erro no modelo {model}: {repr(exc)}")
            erros.append(f"{model}: {exc}")

    raise RuntimeError("HF API indisponivel apos tentativas: " + " | ".join(erros[:4]))


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


def _obter_df_comp_atualizado() -> pd.DataFrame:
    global DF_COMP, DF_COMP_LAST_REFRESH
    try:
        agora = time.time()
        if DF_COMP.empty or (agora - DF_COMP_LAST_REFRESH) >= DF_COMP_REFRESH_INTERVAL_SECONDS:
            novo_df = carregar_comparacao_master_com_precos()
            if not novo_df.empty:
                DF_COMP = novo_df
            DF_COMP_LAST_REFRESH = agora
    except Exception:
        pass
    return DF_COMP

def _obter_df_amz():
    global DF_AMZ
    if DF_AMZ is None:
        DF_AMZ = carregar_amazon_sample()
        if not DF_AMZ.empty and "ASIN" in DF_AMZ.columns:
            DF_AMZ["ASIN"] = DF_AMZ["ASIN"].apply(norm_asin)
    return DF_AMZ if DF_AMZ is not None else pd.DataFrame()


def _obter_df_ml():
    global DF_ML
    if DF_ML is None:
        DF_ML = carregar_ml_sample()
        if not DF_ML.empty and "ASIN" in DF_ML.columns:
            DF_ML["ASIN"] = DF_ML["ASIN"].apply(norm_asin)
    return DF_ML if DF_ML is not None else pd.DataFrame()


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
            "Voce resume opinioes reais de consumidores sobre um produto.\n"
            "Use apenas os comentarios fornecidos.\n"
            "Escreva um unico paragrafo curto, natural e fluido, em portugues do Brasil,\n"
            "como um resumo editorial de 'opinioes em destaque'.\n"
            "O texto deve soar humano, sem cara de relatorio ou analise tecnica.\n"
            "Nao mencione plataforma, marketplace, nota media, quantidade de comentarios ou percentuais.\n"
            "Nao invente fatos e nao copie frases literalmente.\n"
            "Sintetize a percepcao geral, os elogios mais recorrentes e, se existirem, ressalvas pontuais de forma natural.\n\n"
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

    llm = None
    prompt = None

    analise_amz = _analisar_comentarios_plataforma(
        [{"texto": _limpar_meta_texto(t), "nota": "", "data": ""} for t in textos_amz],
        "Amazon",
    )
    analise_ml = _analisar_comentarios_plataforma(
        [{"texto": _limpar_meta_texto(t), "nota": "", "data": ""} for t in textos_ml],
        "Mercado Livre",
    )

    def _contexto_resumo(textos: tuple[str, ...], analise: dict, limite: int = 8, max_chars: int = 2200) -> str:
        itens = []
        total = 0

        for t in textos[:limite]:
            limpo = _limpar_meta_texto(t)
            if not limpo:
                continue
            limpo = re.sub(r"\s+", " ", limpo).strip()
            if len(limpo) > 260:
                limpo = limpo[:260].rstrip() + "..."
            pedaco = f"- {limpo}\n"
            if total + len(pedaco) > max_chars:
                break
            itens.append(pedaco)
            total += len(pedaco)

        return "".join(itens)

    def _ask(textos: tuple[str, ...], analise: dict) -> str:
        # 1) Tenta API remota
        try:
            via_api = _resumo_via_hf_inference_api(textos, analise)
            via_api = re.sub(r"\[[^\]]*\]", "", norm_str(via_api)).strip()
            via_api = _limpar_saida_llm(via_api)

            print("[HF RAW]", via_api[:300])

            if via_api and len(via_api) >= 80:
                return via_api

            return ""
        except Exception as exc:
            print(f"[ERRO HF AMAZON/ML] {exc}")
            return ""

    def _ask_rewrite_from_facts(analise: dict, textos_origem: tuple[str, ...]) -> str:
        nonlocal llm, prompt
        fatos = []
        fatos.append(f"tom: {analise.get('tom', {})}")
        if analise.get("temas_pos"):
            temas_pos = [_tema_para_exibicao(t) for t in analise["temas_pos"]]
            fatos.append(f"elogios recorrentes: {', '.join(temas_pos)}")
        if analise.get("temas_neg"):
            temas_neg = [_tema_para_exibicao(t) for t in analise["temas_neg"]]
            fatos.append(f"pontos de atenção: {', '.join(temas_neg)}")
        if analise.get("media_nota") is not None:
            fatos.append(f"nota média aproximada: {analise['media_nota']}")

        prompt_fatos = (
            "Você é um analista de reviews de produtos.\n"
            "Escreva um resumo em português do Brasil, com 3 a 4 frases, tom natural e profissional.\n"
            "Use apenas os fatos fornecidos, sem inventar dados e sem mencionar marketplace.\n"
            "Não inclua instruções no texto final e não use listas.\n\n"
            f"Fatos extraídos: {'; '.join(fatos)}\n\n"
            "Resumo:"
        )
        try:
            out = _hf_generate_prompt(prompt_fatos, max_new_tokens=140, temperature=0.2)
            if out:
                return out
            if not ALLOW_LOCAL_LLM_FALLBACK:
                return ""
        except Exception as exc:
            print(f"[ERRO HF AMAZON/ML] {exc}")
            if not ALLOW_LOCAL_LLM_FALLBACK:
                return ""

        if llm is None or prompt is None:
            llm, prompt = _get_modelo_ia_resumo()
        result = llm.invoke(prompt_fatos)
        raw = result if isinstance(result, str) else str(result)
        texto = raw.strip()
        if texto.startswith(prompt_fatos):
            texto = texto[len(prompt_fatos):].strip()
        if "Resumo:" in texto:
            texto = texto.split("Resumo:")[-1].strip()
        return _limpar_saida_llm(texto)

    resumo_amz = "Sem comentários suficientes na Amazon."
    if textos_amz:
        resumo_amz = _ask(textos_amz, analise_amz)
        resumo_amz = re.sub(r"\[[^\]]*\]", "", resumo_amz)
        resumo_amz = _deduplicar_frases(resumo_amz)

        if not resumo_amz or len(resumo_amz) < 80:
            resumo_amz = "Resumo de IA indisponível no momento para Amazon."

    resumo_ml = "Sem comentários suficientes no Mercado Livre."
    if textos_ml:
        resumo_ml = _ask(textos_ml, analise_ml)
        resumo_ml = re.sub(r"\[[^\]]*\]", "", resumo_ml)
        resumo_ml = _deduplicar_frases(resumo_ml)

        if not resumo_ml or len(resumo_ml) < 80:
            resumo_ml = "Resumo de IA indisponível no momento para Mercado Livre."

    return {
        "amazon": resumo_amz,
        "ml": resumo_ml,
        "modo": "rag",
    }


def gerar_resumo_comentarios(comentarios: dict) -> dict:
    """Orquestra resumo com IA real (HF/LangChain) e cache por assinatura."""
    textos_amz = tuple(_extrair_textos_comentarios(comentarios.get("amazon", [])))
    textos_ml = tuple(_extrair_textos_comentarios(comentarios.get("ml", [])))

    if not textos_amz and not textos_ml:
        return {
            "amazon": "Sem comentários da Amazon para resumir.",
            "ml": "Sem comentários do Mercado Livre para resumir.",
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
            "amazon": "Resumo de IA indisponível no momento para Amazon.",
            "ml": "Resumo de IA indisponível no momento para Mercado Livre.",
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


def buscar_preco_amazon(asin):
    a = norm_asin(asin)
    if not a:
        return "Não disponível"

    df_amz = _obter_df_amz()
    if df_amz.empty or "ASIN" not in df_amz.columns:
        return "Não disponível"

    try:
        df_match = df_amz[df_amz["ASIN"] == a]
        if not df_match.empty and "Preço" in df_match.columns:
            val = df_match.iloc[-1].get("Preço", "")
            if val and str(val).strip().lower() != "nan":
                return str(val)
    except Exception:
        pass

    return "Não disponível"


def buscar_preco_ml(asin, link):
    asin = norm_asin(asin)
    link = norm_str(link)

    df_ml = _obter_df_ml()
    if df_ml.empty:
        return "Não disponível"

    try:
        if asin and "ASIN" in df_ml.columns:
            df_asin = df_ml[df_ml["ASIN"] == asin]
            if not df_asin.empty:
                val = df_asin.iloc[-1].get("Preço à vista", "") or df_asin.iloc[-1].get("Preço", "")
                if val and str(val).strip().lower() != "nan":
                    return str(val)

        if link and "Link" in df_ml.columns:
            df_link = df_ml[df_ml["Link"].astype(str).str.strip() == link]
            if not df_link.empty:
                val = df_link.iloc[-1].get("Preço à vista", "") or df_link.iloc[-1].get("Preço", "")
                if val and str(val).strip().lower() != "nan":
                    return str(val)
    except Exception:
        pass

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
    df = _obter_df_amz().copy()
    df["Subcategoria"] = df["Subcategoria"].apply(limpar_categoria)
    df = df[df["Subcategoria"] != ""]
    categorias = ["Todas"] + sorted(df["Subcategoria"].unique().tolist())

    categoria = request.args.get("categoria", "Todas")
    produto = request.args.get("produto", "")

    df_filtrado = _obter_df_comp_atualizado().copy()
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
    df_comp_copy = _obter_df_comp_atualizado().copy()
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
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)
