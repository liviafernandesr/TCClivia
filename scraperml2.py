from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
from datetime import datetime
import os
import time
from urllib.parse import urlparse
import re
from selenium.webdriver.chrome.service import Service 
from webdriver_manager.chrome import ChromeDriverManager
import pickle

COOKIES_PATH = "cookies_ml.pkl"
url = "https://www.mercadolivre.com.br/mais-vendidos" 

# Configuração do Chrome (otimizado para velocidade MAS COM IMAGENS)
options = Options()
# options.add_argument("--headless")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-notifications")
options.add_argument("--disable-gpu")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--no-sandbox")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
# REMOVI A LINHA QUE DESABILITA IMAGENS - MANTENDO IMAGENS HABILITADAS

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

def carregar_cookies(driver):
    if os.path.exists(COOKIES_PATH):
        with open(COOKIES_PATH, "rb") as file:
            cookies = pickle.load(file)
            for cookie in cookies:
                driver.add_cookie(cookie)
        return True
    return False

def formatar_data(data_original):
    try:
        meses = {
            'jan.': '01', 'fev.': '02', 'mar.': '03', 'abr.': '04', 'mai.': '05', 'jun.': '06',
            'jul.': '07', 'ago.': '08', 'set.': '09', 'out.': '10', 'nov.': '11', 'dez.': '12'
        }
        partes = data_original.replace("de ", "").split()
        dia, mes, ano = partes[0], partes[1], partes[2]
        return f"{dia.zfill(2)}/{meses[mes]}/{ano}"
    except:
        return data_original

def encurtar_link(link):
    try:
        parsed = urlparse(link)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    except:
        return link
    
def extrair_nota_individual(comentario_element):
    try:
        # NO IFRAME, usar os seletores corretos que aparecem no seu HTML
        # Contar estrelas preenchidas dentro do elemento de comentário
        estrelas_cheias = comentario_element.find_elements(By.CSS_SELECTOR, 
            "div.ui-review-capability-comments__comment__rating use[href='#poly_star_fill'], "
            "svg.ui-review-capability-comments__comment__rating__star use[href='#poly_star_fill']"
        )
        
        # Contar estrelas meio preenchidas
        estrelas_meia = comentario_element.find_elements(By.CSS_SELECTOR,
            "div.ui-review-capability-comments__comment__rating use[href='#poly_star_half'], "
            "svg.ui-review-capability-comments__comment__rating__star use[href='#poly_star_half']"
        )
        
        nota = len(estrelas_cheias)
        
        # Se tem estrela meia, adicionar 0.5
        if estrelas_meia:
            nota += 0.5
        
        # Se não encontrou estrelas, tentar extrair do texto oculto
        if nota == 0:
            try:
                texto_oculto = comentario_element.find_element(By.CSS_SELECTOR, "p.andes-visually-hidden").text
                # Procurar padrão "Avaliação X de 5"
                match = re.search(r"Avaliação\s+(\d+(?:[.,]\d+)?)\s+de\s+5", texto_oculto)
                if match:
                    nota_str = match.group(1).replace(",", ".")
                    nota = float(nota_str)
            except:
                pass
        
        return str(nota) if nota > 0 else "N/A"
    except Exception as e:
        print(f"Erro na nota individual: {e}")
        return "N/A"

def extrair_preco_avista(driver):
    try:
        price_container = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.ui-pdp-price__main, div.ui-pdp-price__second-line"))
        )
        
        preco_element = price_container.find_element(By.CSS_SELECTOR, "span.andes-money-amount__fraction")
        centavos_element = price_container.find_elements(By.CSS_SELECTOR, "span.andes-money-amount__cents")
        
        preco = preco_element.text.replace(".", "")
        centavos = centavos_element[0].text if centavos_element else "00"
        
        return f"{preco},{centavos}"
            
    except:
        try:
            preco_fallback = driver.find_element(By.CSS_SELECTOR, "span.andes-money-amount__fraction").text.replace(".", "")
            centavos_fallback_elements = driver.find_elements(By.CSS_SELECTOR, "span.andes-money-amount__cents")
            centavos_fallback = centavos_fallback_elements[0].text if centavos_fallback_elements else "00"
            return f"{preco_fallback},{centavos_fallback}"
        except:
            return "Preço não identificado"

def extrair_asin(link):
    try:
        # Padrões comuns de ASIN no Mercado Livre Brasil
        padroes = [
            r"MLB[-\s]?\d+",           # MLB-12345678 ou MLB 12345678
            r"ML[A-Z]\d+",             # MLI12345678, MLC12345678, etc
            r"([A-Z]{3}\d+)",          # 3 letras + números
            r"-([A-Z0-9]{10,13})$",    # Código no final da URL
            r"-([A-Z0-9]{10,13})\?",   # Código antes de parâmetros
            r"-([A-Z0-9]{10,13})/"     # Código antes de trailing slash
        ]
        
        for padrao in padroes:
            match = re.search(padrao, link)
            if match:
                asin = match.group(1) if len(match.groups()) > 0 else match.group()
                # Limpar caracteres especiais
                asin = re.sub(r'[^A-Z0-9]', '', asin)
                if len(asin) >= 10:  # ASINs geralmente têm 10+ caracteres
                    return asin
        
        # Se nenhum padrão funcionar, tentar pegar os últimos números/letras
        url_parts = link.split('/')
        for part in reversed(url_parts):
            # Procurar por MLB- seguido de números
            if 'MLB-' in part or 'MLB_' in part or 'MLB ' in part:
                cleaned = re.sub(r'[^A-Z0-9-]', '', part)
                return cleaned
            
            # Se tiver MLB no começo
            if part.startswith('MLB'):
                return part
        
        # Último recurso: pegar a última parte da URL
        last_part = url_parts[-1].split('?')[0]  # Remove parâmetros
        if last_part and len(last_part) >= 8:
            return last_part
            
    except Exception as e:
        print(f"Erro ao extrair ASIN: {e}")
    
    return "N/A"

def extrair_nota_geral_e_avaliacoes(driver):
    try:
        review_element = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a.ui-pdp-review__label"))
        )
        nota_geral = review_element.find_element(By.CSS_SELECTOR, "span.ui-pdp-review__rating").text
        nota_geral = nota_geral.replace(".", ",") if nota_geral else "N/A"
        qtd_avaliacoes = review_element.find_element(By.CSS_SELECTOR, "span.ui-pdp-review__amount").text
        qtd_avaliacoes = qtd_avaliacoes.replace("(", "").replace(")", "").replace(".", "").strip()
        return nota_geral, qtd_avaliacoes
    except:
        return "N/A", "N/A"

def extrair_data_comentario(comentario_element):
    try:
        # No seu HTML, a data está em: <span class="ui-review-capability-comments__comment__date">20 mai. 2023</span>
        data_element = comentario_element.find_element(By.CSS_SELECTOR, "span.ui-review-capability-comments__comment__date")
        data_original = data_element.text.strip()
        
        # Formatar a data
        return formatar_data(data_original)
    except:
        try:
            # Tentar outro seletor alternativo
            data_element = comentario_element.find_element(By.CSS_SELECTOR, "span[class*='date'], time")
            data_original = data_element.text.strip()
            return formatar_data(data_original)
        except:
            return "N/A"
        
        
def extrair_texto_comentario(comentario_element):
    try:
        # No seu HTML, o texto está em: <p class="ui-review-capability-comments__comment__content">
        texto_element = comentario_element.find_element(By.CSS_SELECTOR, 
            "p.ui-review-capability-comments__comment__content, "
            "p[data-testid='comment-content-component']"
        )
        texto = texto_element.text.strip()
        return texto if texto else "Sem texto"
    except:
        try:
            # Tentar encontrar qualquer texto de comentário
            textos = comentario_element.find_elements(By.TAG_NAME, "p")
            for p in textos:
                texto = p.text.strip()
                if texto and len(texto) > 5:  # Pelo menos 5 caracteres
                    return texto
            return "Texto não disponível"
        except:
            return "Texto não disponível"

def scroll_ate_final(driver):
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1.5)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def extrair_caracteristicas(driver):
    caracteristicas = {}

    try:
        pickers = driver.find_elements(
            By.CLASS_NAME, "ui-pdp-outside_variations__picker"
        )

        for picker in pickers:
            try:
                titulo = picker.find_element(
                    By.CLASS_NAME, "ui-pdp-outside_variations__title"
                )

                nome = titulo.find_element(
                    By.CLASS_NAME, "ui-pdp-outside_variations__title__label"
                ).text.replace(":", "").strip()

                valor = titulo.find_element(
                    By.CLASS_NAME, "ui-pdp-outside_variations__title__value"
                ).text.strip()

                caracteristicas[nome] = valor
            except:
                continue

    except:
        pass

    return caracteristicas


# Carregar cookies (MAIS RÁPIDO)
driver.get("https://www.mercadolivre.com.br")
carregar_cookies(driver)
driver.refresh()
time.sleep(1)  # REDUZIDO

driver.get(url)
time.sleep(1)  # REDUZIDO

print("🔍 Buscando todas as categorias do bloco final...")

dados = []
posicao_global = 1

scroll_ate_final(driver)

# =======================================================
# LOOP POR CADA SUBCATEGORIA - CORRIGIDO
# =======================================================
for idx in range(50):  # Limite seguro de categorias
    try:
        # SEMPRE VOLTAR PARA A PÁGINA INICIAL DAS CATEGORIAS
        if driver.current_url != url:
            driver.get(url)
            time.sleep(2)
            scroll_ate_final(driver)
        
        # Aguardar container de categorias
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.CategoryList"))
        )
        
        # Re-coletar categorias a cada iteração
        categorias = container.find_elements(By.CSS_SELECTOR, "a")
        
        if idx >= len(categorias):
            print(f"\n✅ Todas as {len(categorias)} categorias processadas!")
            break
            
        categoria = categorias[idx]
        subcategoria = categoria.text.strip()
        
        if not subcategoria:  # Pular categorias sem nome
            continue
            
        print("\n" + "=" * 60)
        print(f"🟦 PROCESSANDO SUBCATEGORIA {idx+1}")
        print(f"   📌 Nome: {subcategoria}")
        print("=" * 60)

        # Obter link ANTES de clicar (mais seguro)
        link_categoria = categoria.get_attribute("href")
        if not link_categoria:
            print("   ⚠ Link da categoria não encontrado, pulando...")
            continue
            
        # Navegar diretamente para o link da categoria
        driver.get(link_categoria)
        time.sleep(2)
        
        print(f"   🏷️ Título da página: {driver.title}")
        print(f"   📍 Subcategoria ativa: {subcategoria}")
        print("   🔍 Carregando todos os produtos...")

        # Aguardar produtos carregarem
        try:
            produtos = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.poly-component__title"))
            )
            print(f"   ✅ Total de {len(produtos)} produtos encontrados")
        except Exception as e:
            print(f"   ⚠ Nenhum produto encontrado: {e}")
            continue
        
        # =======================================================
        # LOOP POR TODOS OS PRODUTOS (mesmo código)
        # =======================================================
        for posicao, prod in enumerate(produtos, 1):
            print(f"\n   📦 Processando produto {posicao}/{len(produtos)}")
            
            try:
                nome = prod.text.strip()
                if not nome:
                    continue
                    
                link = encurtar_link(prod.get_attribute("href"))
                if not link or not link.startswith("http"):
                    continue

                # Abrir produto em nova aba
                driver.execute_script(f"window.open('{link}');")
                driver.switch_to.window(driver.window_handles[1])
                time.sleep(1)

                # Extrair dados básicos
                try:
                    preco = extrair_preco_avista(driver)
                except:
                    preco = "Preço não identificado"
                
                asin = extrair_asin(link)
                print(f"      🔗 ASIN extraído: {asin} (do link: {link})")
                # Log para debugging
                if asin == "N/A":
                    print(f"      ⚠️ ATENÇÃO: ASIN não extraído do link: {link}")
                
                try:
                    nota_geral, qtd_avaliacoes = extrair_nota_geral_e_avaliacoes(driver)
                except:
                    nota_geral, qtd_avaliacoes = "N/A", "N/A"
                
                try:
                    caracteristicas = extrair_caracteristicas(driver)
                except:
                    caracteristicas = "Não Possui"

                # ======================================================
                # COLETAR COMENTÁRIOS
                # ======================================================
                comentarios = []
                comentarios_coletados = 0

                # 1️⃣ Clicar nas estrelas
                try:
                    estrelas = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "a.ui-pdp-review__label"))
                    )
                    driver.execute_script("arguments[0].click();", estrelas)
                    print("      ⭐ Avaliações clicadas (estrelas)")
                except:
                    print("      ⚠ Não foi possível clicar nas estrelas")

                time.sleep(2)

                # 2️⃣ Clicar em "Mostrar todas as opiniões"
                try:
                    botao_mais = WebDriverWait(driver, 4).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='see-more']"))
                    )
                    driver.execute_script("arguments[0].click();", botao_mais)
                    print("      🔎 Botão 'Mostrar todas as opiniões' clicado")
                    time.sleep(2)
                except:
                    print("      ℹ️ Botão 'Mostrar todas' não encontrado (ok)")

                # Detectar iframe
                iframe_encontrado = False
                iframes = driver.find_elements(By.TAG_NAME, "iframe")

                for iframe in iframes:
                    src = iframe.get_attribute("src") or ""
                    if "reviews" in src or "opini" in src:
                        driver.switch_to.frame(iframe)
                        iframe_encontrado = True
                        print("      ✅ Avaliações renderizadas via iframe")
                        break

                if not iframe_encontrado:
                    print("      ✅ Avaliações renderizadas direto no DOM")

                # 3️⃣ Aguardar seção principal
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "section[data-testid='reviews-desktop']"))
                    )
                except:
                    print("      ⚠ Seção de reviews não carregou")

                # 4️⃣ Scroll para carregar comentários
                for _ in range(5):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(0.8)

                # 5️⃣ Coletar até 10 comentários
                comentarios_elementos = driver.find_elements(
                    By.CSS_SELECTOR, "article.ui-review-capability-comments__comment"
                )

                for comentario_el in comentarios_elementos[:10]:
                    texto = extrair_texto_comentario(comentario_el)
                    nota = extrair_nota_individual(comentario_el)
                    data_comentario = extrair_data_comentario(comentario_el)

                    comentarios.append({
                        "Comentário": texto,
                        "Nota": nota,
                        "Data Comentário": data_comentario
                    })
                    comentarios_coletados += 1

                driver.switch_to.default_content()

                print(f"      📝 Nome: {nome[:80]}{'...' if len(nome) > 80 else ''}")
                print(f"      💰 Preço: {preco}")
                print(f"      ⭐ Nota: {nota_geral}")
                print(f"      📊 Avaliações: {qtd_avaliacoes}")
                
                if caracteristicas:
                    print("      🔧 Características:")
                    for nome_carac, valor in caracteristicas.items():
                        print(f"         - {nome_carac}: {valor}")
                else:
                    print("      🔧 Características: Nenhuma")
                    
                print(f"      🗨️ Comentários coletados: {comentarios_coletados}")

                # Adicionar cada comentário como uma linha separada
                for comentario in comentarios:
                    dados.append({
                        "Posição Global": posicao_global,
                        "Posição Categoria": posicao,
                        "Subcategoria": subcategoria,
                        "ASIN": asin,
                        "Nome": nome,
                        "Preço à vista": preco,
                        "Nota Geral": nota_geral,
                        "Qtd. Avaliações": qtd_avaliacoes,
                        "Caractéristicas": caracteristicas,
                        "País": "Brasil",
                        "Nota Comentário": comentario["Nota"],
                        "Data Comentário": comentario["Data Comentário"],
                        "Comentário": comentario["Comentário"],
                        "Link": link
                    })

                print(f"      ✅ Produto finalizado com sucesso")
                posicao_global += 1

            except Exception as e:
                print(f"      ❌ Erro no produto {posicao}: {str(e)[:100]}...")
            finally:
                # Fechar aba do produto SEMPRE
                if len(driver.window_handles) > 1:
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                    time.sleep(0.5)

    except Exception as e:
        print(f"\n❌ Erro na categoria {idx+1}: {str(e)[:100]}...")
        print("   Continuando para próxima categoria...")
        continue

# Resto do código permanece igual...

# Salvar resultados
if dados:
    df = pd.DataFrame(dados)
    os.makedirs("resultados", exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-d_%H-%M")
    
    # Arquivo 1: FULL (Todos os comentários)
    file_full = f"data/resultados_ml/mais_vendidos_ml_FULL_{ts}.csv"
    df.to_csv(file_full, index=False, encoding="utf-8-sig", sep=";")
    
    # Arquivo 2: RESUMO (Sem duplicatas de ASIN, apenas 1 linha por produto)
    file_resumo = f"data/resultados_ml/mais_vendidos_ml_SAMPLE_{ts}.csv"
    
    # Criar resumo com apenas os dados básicos (primeiro comentário de cada produto)
    df_resumo = df.drop_duplicates(subset=['ASIN', 'Nome'], keep='first')
    df_resumo.to_csv(file_resumo, index=False, encoding="utf-8-sig", sep=";")
    
    print("\n" + "=" * 60) 
    print("🎉 SCRAPING FINALIZADO COM SUCESSO")
    print(f"📦 Total de produtos processados: {posicao_global - 1}")
    print(f"📝 Total de registros coletados: {len(df)}")
    print("=" * 60)

    print(f"\n🎉 Relatórios gerados:")
    print(f"1. {file_full} - {len(df)} registros")
    print(f"2. {file_resumo} - {len(df_resumo)} produtos únicos")
else:
    print("⚠️ Nenhum dado coletado.")

driver.quit()