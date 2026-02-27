import streamlit as st
import pandas as pd

from utils.loaders import (
    carregar_comparacao,
    carregar_amazon_sample,
    carregar_ml_sample
)

st.set_page_config(page_title="Comparador de Avaliações", page_icon="🛒", layout="wide")

st.markdown("""
# 🛒 Comparador de Avaliações de Produtos
Centralização de avaliações de múltiplos e-commerces
""")
st.divider()

# -----------------------------
# HELPERS
# -----------------------------
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
    # remove categorias lixo muito curtas (ex.: "s")
    if len(c) < 3:
        return ""
    return c

def preco_para_float(preco):
    try:
        p = str(preco).replace("R$", "").strip()
        p = p.replace(".", "").replace(",", ".")
        return float(p)
    except:
        return None

def botao_link(texto, url):
    """Substitui st.link_button (pra evitar bug e não precisar key)."""
    url = norm_str(url)
    if not url:
        return
    st.markdown(f"<a href='{url}' target='_blank'><button>{texto}</button></a>", unsafe_allow_html=True)

# -----------------------------
# LOAD
# -----------------------------
@st.cache_data
def load_data():
    return (
        carregar_comparacao(),
        carregar_amazon_sample(),
        carregar_ml_sample()
    )

df_comp, df_amz, df_ml = load_data()

if df_comp.empty:
    st.error("Base de comparação não encontrada.")
    st.stop()

if df_amz.empty:
    st.error("Sample da Amazon não encontrado.")
    st.stop()

if df_ml.empty:
    st.error("Sample do Mercado Livre não encontrado.")
    st.stop()

# -----------------------------
# CATEGORIAS: SÓ do SAMPLE AMAZON (Subcategoria)
# -----------------------------
df_amz["Subcategoria"] = df_amz["Subcategoria"].apply(limpar_categoria)
df_amz = df_amz[df_amz["Subcategoria"] != ""]

categorias = ["Todas"] + sorted(df_amz["Subcategoria"].unique().tolist())

# -----------------------------
# LOOKUPS DE PREÇO: SÓ DOS SAMPLES
# -----------------------------
df_amz["ASIN"] = df_amz["ASIN"].apply(norm_asin)
df_ml["ASIN"] = df_ml["ASIN"].apply(norm_asin)

# Amazon: ASIN -> Preço
amz_price_by_asin = (
    df_amz.dropna(subset=["ASIN"])
          .drop_duplicates(subset=["ASIN"], keep="last")
          .set_index("ASIN")["Preço"]
          .astype(str)
          .to_dict()
)

# ML: ASIN -> Preço à vista
ml_price_by_asin = (
    df_ml.dropna(subset=["ASIN"])
         .drop_duplicates(subset=["ASIN"], keep="last")
         .set_index("ASIN")["Preço à vista"]
         .astype(str)
         .to_dict()
)

# ML fallback: Link -> Preço à vista (se o sample tiver Link)
ml_price_by_link = {}
if "Link" in df_ml.columns:
    df_ml["Link"] = df_ml["Link"].apply(norm_str)
    ml_price_by_link = (
        df_ml[df_ml["Link"] != ""]
            .drop_duplicates(subset=["Link"], keep="last")
            .set_index("Link")["Preço à vista"]
            .astype(str)
            .to_dict()
    )

def buscar_preco_amazon(asin_amz):
    return amz_price_by_asin.get(norm_asin(asin_amz), "Não disponível")

def buscar_preco_ml(asin_ml, link_ml):
    asin = norm_asin(asin_ml)
    if asin and asin.lower() != "nan":
        p = ml_price_by_asin.get(asin)
        if p and str(p).lower() != "nan":
            return p

    link = norm_str(link_ml)
    if link:
        p = ml_price_by_link.get(link)
        if p and str(p).lower() != "nan":
            return p

    return "Não disponível"

# -----------------------------
# SIDEBAR
# -----------------------------
st.sidebar.header("🔍 Busca")
categoria = st.sidebar.selectbox("Categoria (Amazon)", categorias)

df_filtrado = df_comp.copy()
df_filtrado["ASIN Amazon"] = df_filtrado["ASIN Amazon"].apply(norm_asin)

if categoria != "Todas":
    asins_categoria = df_amz[df_amz["Subcategoria"] == categoria]["ASIN"].unique()
    df_filtrado = df_filtrado[df_filtrado["ASIN Amazon"].isin(asins_categoria)]

df_filtrado["Produto Amazon"] = df_filtrado["Produto Amazon"].apply(norm_str)
produtos = sorted([p for p in df_filtrado["Produto Amazon"].unique().tolist() if p])

produto = st.sidebar.selectbox("Produto", ["Selecione..."] + produtos)

if produto != "Selecione...":
    df_filtrado = df_filtrado[df_filtrado["Produto Amazon"] == produto]

# -----------------------------
# RESULTADOS
# -----------------------------
st.subheader("📦 Produtos encontrados")

if df_filtrado.empty:
    st.warning("Nenhum produto encontrado.")
    st.stop()

for _, row in df_filtrado.iterrows():
    st.markdown(f"### {row['Produto Amazon']}")

    with st.container(border=True):
        st.metric("📊 Nota Geral do Produto", f"{row['Nota Geral Ponderada']:.2f}")

    st.write("")
    c1, c2 = st.columns(2, gap="large")

    asin_amz = row["ASIN Amazon"]
    asin_ml = row.get("ASIN Mercado Livre", "")
    link_ml = row.get("Link Mercado Livre", "")

    preco_am = buscar_preco_amazon(asin_amz)
    preco_ml = buscar_preco_ml(asin_ml, link_ml)

    # LINKS (do df_comp, como você pediu)
    link_am = row.get("Link Amazon", "")
    link_ml_btn = row.get("Link Mercado Livre", "")

    with c1:
        st.markdown("#### 🛍️ Amazon")
        with st.container(border=True):
            st.metric("⭐ Nota geral", f"{row['Nota Amazon']:.2f}")
            st.metric("🧾 Qtd. avaliações", row["Avaliações Amazon"])
            st.metric("💰 Preço", preco_am)
        botao_link("Comprar na Amazon", link_am)

    with c2:
        st.markdown("#### 📦 Mercado Livre")
        with st.container(border=True):
            st.metric("⭐ Nota geral", f"{row['Nota Mercado Livre']:.2f}")
            st.metric("🧾 Qtd. avaliações", row["Avaliações ML"])
            st.metric("💰 Preço", preco_ml)
        botao_link("Comprar no Mercado Livre", link_ml_btn)

    pa = preco_para_float(preco_am)
    pm = preco_para_float(preco_ml)

    if pa is not None and pm is not None:
        if pa < pm:
            st.success("🏆 Melhor opção por preço: **Amazon**")
        elif pm < pa:
            st.success("🏆 Melhor opção por preço: **Mercado Livre**")
        else:
            st.info("⚖️ Empate de preço")
    else:
        st.info("ℹ️ Não foi possível comparar preços.")

    st.divider()