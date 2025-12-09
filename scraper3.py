from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pickle
import pandas as pd
import time
import os
from datetime import datetime
import locale
import re

# Tenta definir o formato de datas para português. Se não funcionar, ignora o erro.
try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
except:
    pass 

# Define o caminho dos cookies salvos, o número máximo de produtos a extrair e o link da categoria.
COOKIES_PATH = "cookies_amazon.pkl"
MAX_PRODUTOS = 5
CATEGORIA_URL = "https://www.amazon.com.br/gp/bestsellers/electronics/16243890011"

def carregar_cookies(driver):
    with open(COOKIES_PATH, "rb") as file:
        cookies = pickle.load(file)
        for cookie in cookies:
            if "sameSite" in cookie:
                cookie.pop("sameSite")
            driver.add_cookie(cookie)

def extrair_asin_dos_produtos(driver):
    """
    Extrai os ASINs (Amazon Standard Identification Numbers) dos produtos
    mais vendidos de uma página de categoria da Amazon.

    Args:
        driver: O objeto do navegador Selenium.

    Returns:
        Uma lista de ASINs encontrados.
    """
    print("🔍 Extraindo ASINs dos produtos mais vendidos...")
    driver.get(CATEGORIA_URL)
    time.sleep(3)
    
    # Rola a página para garantir que os itens sejam carregados
    for _ in range(2):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    asins = set()
    
    # Encontra todos os links de produtos que contêm "/dp/" no href.
    # Esta é uma forma mais confiável de encontrar os produtos.
    links_produtos = soup.select('a[href*="/dp/"]')
    
    for link in links_produtos:
        href = link.get('href', '')
        
        # Usa regex para extrair o ASIN de 10 caracteres do URL
        match = re.search(r'/dp/([A-Z0-9]{10})', href)
        if match:
            asin = match.group(1)
            # Adiciona o ASIN ao conjunto para evitar duplicatas
            asins.add(asin)
            
    return list(asins)

# ==============================================================================
# FUNÇÃO DE DATA MODIFICADA PARA RETORNAR PAÍS E DATA SEPARADAMENTE
# ==============================================================================
def formatar_data_e_pais_amazon(data_str):
    try:
        # Usa regex para encontrar o país e a data
        match = re.search(r"Avaliado no (.+?) em (.+)", data_str)
        if match:
            pais = match.group(1).strip()
            data_limpa = match.group(2).strip()
            
            # Converte para objeto datetime e depois para string DD/MM/YYYY
            data_obj = datetime.strptime(data_limpa, "%d de %B de %Y")
            return pais, data_obj.strftime('%d/%m/%Y')
        # Se não encontrar o padrão, retorna N/A
        return "N/A", data_str
    except:
        return "N/A", data_str

def extrair_qtd_avaliacoes(soup):
    try:
        qtd_tag = soup.find('span', {'id': 'acrCustomerReviewText'})
        if qtd_tag:
            texto = qtd_tag.get_text(strip=True)
            qtd = ''.join(filter(str.isdigit, texto.split()[0]))
            return f"{int(qtd):,}".replace(",", ".")
        
        qtd_fallback = soup.select_one('div[data-hook="total-review-count"]')
        if qtd_fallback:
            return f"{int(qtd_fallback.get_text(strip=True).split()[0].replace('.', '')):,}".replace(",", ".")
        
        return "N/A"
    except Exception as e:
        print(f"⚠ Erro ao extrair qtd. avaliações: {e}")
        return "N/A"
    
def extrair_detalhes_produto(driver, asin):
    url = f"https://www.amazon.com.br/dp/{asin}"
    driver.get(url)
    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    nome_produto_tag = soup.select_one('#productTitle')
    nome_produto = nome_produto_tag.get_text(strip=True) if nome_produto_tag else "Nome não encontrado"

    # PREÇO MODIFICADO PARA RETORNAR APENAS O VALOR NUMÉRICO
    preco_tag = soup.select_one('span.a-price span.a-offscreen')
    preco = preco_tag.get_text(strip=True).replace("R$", "").replace(".", "").strip() if preco_tag else "Preço não disponível"

    nota_geral_tag = soup.select_one('#averageCustomerReviews span.a-icon-alt')
    nota_geral = nota_geral_tag.get_text(strip=True).split()[0].replace(".", ",") if nota_geral_tag else "N/A"

    qtd_avaliacoes = extrair_qtd_avaliacoes(soup)

    return nome_produto, preco, nota_geral, qtd_avaliacoes

def extrair_comentarios(html, asin, nome_produto, preco, nota_geral):
    soup = BeautifulSoup(html, 'html.parser')
    comentarios = []

    # 1. Busca FORÇADA pelos blocos de comentários no elemento <li> com data-hook="review".
    blocos = soup.find_all("li", {"data-hook": "review"}) 
    
    # Se, por algum motivo, o <li> for pulado, o fallback para a <div> interna com ID é mantido.
    if not blocos:
        blocos = soup.find_all("div", id=lambda x: x and x.startswith("customer_review-"))
        
    if not blocos:
        print("Aviso: Nenhum bloco de comentário principal encontrado com os seletores esperados.")
        return []

    for bloco in blocos:
        # 2. Extrai a Nota (Tag <i> com data-hook='review-star-rating' e o <span> dentro)
        nota_tag = bloco.select_one("i[data-hook='review-star-rating'] span")

        # 3. Extrai o Corpo do Texto.
        # O seletor mais seguro é a <span> dentro da div 'review-collapsed'.
        # Se for um comentário curto (sem 'Ler mais'), ele estará no span direto no 'review-body'.
        texto_container = bloco.select_one("div[data-hook='review-collapsed'] span")
        
        # Fallback para textos curtos sem o div de expander
        if not texto_container:
            texto_container = bloco.select_one("span[data-hook='review-body'] span")
            
        # 4. Extrai a Data
        data_tag = bloco.select_one("span[data-hook='review-date']")

        # Garante que a data está presente antes de tentar processar
        data_str = data_tag.get_text(strip=True) if data_tag else "N/D"
        pais, data_formatada = formatar_data_e_pais_amazon(data_str)

        # Adiciona o comentário
        comentarios.append({
            "ASIN": asin,
            "Nome": nome_produto,
            "Preço": preco,
            "Nota Geral": nota_geral,
            "País": pais, 
            "Data Comentário": data_formatada,
            # Limpa o texto da nota: ex: "5,0 de 5 estrelas" -> "5,0"
            "Nota": nota_tag.get_text(strip=True).split()[0].replace(",",",") if nota_tag else "N/A",
            "Comentário": texto_container.get_text(strip=True) if texto_container else "Sem texto",
            "Link": f"https://www.amazon.com.br/dp/{asin}"
        })

    return comentarios

def get_comentarios_produto(driver, asin, nome_produto, preco, nota_geral):
    url = f"https://www.amazon.com.br/product-reviews/{asin}/"
    for _ in range(2):
        try:
            driver.get(url)
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            return extrair_comentarios(driver.page_source, asin, nome_produto, preco, nota_geral)
        except Exception:
            time.sleep(3) 
    return []

def main():
    if not os.path.exists(COOKIES_PATH):
        print("❌ Cookies não encontrados. Rode 'login_amazon.py' primeiro.")
        return

    options = Options()
    options.add_argument("--window-size=1200,800") 
    # options.add_argument("--headless") 
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        print("🔄 Iniciando navegador e carregando cookies...")
        driver.get("https://www.amazon.com.br")
        carregar_cookies(driver)
        time.sleep(2)

        print("\n🔍 Buscando produtos mais vendidos em Eletrônicos...")
        asins = extrair_asin_dos_produtos(driver)
        print(f"✅ {len(asins)} produtos encontrados: {asins}")

        todos_comentarios = []
        asin_para_posicao = {asin: i+1 for i, asin in enumerate(asins)}

        for i, asin in enumerate(asins, 1):
            print(f"\n📦 Processando produto {i}/{len(asins)} (ASIN: {asin})")
            try:
                nome_produto, preco, nota_geral, qtd_avaliacoes = extrair_detalhes_produto(driver, asin)
                # comentarios = get_comentarios_produto(driver, asin, nome_produto, preco, nota_geral)
                
                # if comentarios:
                #     for comentario in comentarios:
                #         comentario["Posição"] = asin_para_posicao[asin]
                #         comentario["Qtd. Avaliações"] = qtd_avaliacoes
                #     todos_comentarios.extend(comentarios)
                #     print(f"   ✅ {len(comentarios)} comentários coletados (Posição: {asin_para_posicao[asin]})")
                # 2. Adiciona a linha do produto com valores vazios para os comentários
                todos_comentarios.append({
                    "ASIN": asin,
                    "Nome": nome_produto,
                    "Preço": preco,
                    "Nota Geral": nota_geral,
                    "Qtd. Avaliações": qtd_avaliacoes,
                    "Posição": asin_para_posicao[asin],
                    "País": "N/A", 
                    "Data Comentário": "N/A",
                    "Nota": "N/A",
                    "Comentário": "COLETA DE COMENTÁRIOS IGNORADA",
                    "Link": f"https://www.amazon.com.br/dp/{asin}"
                })
                print(f"   ✅ Detalhes coletados. Coleta de comentários intencionalmente pulada.")
            except Exception as e:
                print(f"❌ Erro ao processar produto {asin}: {e}")

        if todos_comentarios:
            df = pd.DataFrame(todos_comentarios)
            # Adiciona a coluna "País" na ordem desejada
            colunas = ['Posição', 'ASIN', 'Nota Geral', 'Qtd. Avaliações', 'País', 'Data Comentário', 'Nome', 'Preço', 'Nota', 'Comentário', 'Link']
            
            # Garante que todas as colunas existam antes de reordenar
            for col in colunas:
                if col not in df.columns:
                    df[col] = "N/A"
            df = df[colunas].sort_values(by="Posição")

            os.makedirs("resultados", exist_ok=True)
            data_hora = datetime.now().strftime("%Y-%m-%d_%H-%M")
            arquivo_csv = f"resultados/comentarios_eletr_amz_{data_hora}.csv"
            df.to_csv(arquivo_csv, index=False, encoding='utf-8-sig', sep=';')

            print(f"\n🎉 Relatório completo salvo em: {arquivo_csv}")
            print(f"📊 Total de comentários: {len(df)}")
            print(f"📦 Produtos analisados: {len(asins)}")
        else:
            print("\n⚠️ Nenhum comentário foi coletado. Verifique a conexão ou os seletores.")
    except Exception as e:
        print(f"\n❌ Erro durante a execução: {str(e)}")
    finally:
        driver.quit()
        print("\n🛑 Navegador fechado")

if __name__ == "__main__":
    main()
