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
        asin = re.search(r"(MLB-?\d+)", link).group()
        return asin
    except:
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

# === PARTE PRINCIPAL ===
if not os.path.exists(COOKIES_PATH):
    print("❌ Cookies não encontrados!")
    driver.quit()
    exit()

# Carregar cookies (MAIS RÁPIDO)
driver.get("https://www.mercadolivre.com.br")
carregar_cookies(driver)
driver.refresh()
time.sleep(1)  # REDUZIDO

driver.get(url)
time.sleep(1)  # REDUZIDO

# Identificar TODAS as subcategorias
sections = driver.find_elements(By.CSS_SELECTOR, "section.dynamic-carousel-normal-desktop")

print(f"📌 {len(sections)} subcategorias encontradas.")

dados = []
posicao_global = 1

# =======================================================
# LOOP POR CADA SUBCATEGORIA
# =======================================================
for i in range(len(sections)):
    # Voltar para a página principal
    driver.get(url)
    time.sleep(1)
    
    # Recarregar todas as seções
    sections = driver.find_elements(By.CSS_SELECTOR, "section.dynamic-carousel-normal-desktop")
    if i >= len(sections):
        continue
        
    section = sections[i]

    # Extrair informações da subcategoria
    try:
        subcategoria = section.find_element(By.CSS_SELECTOR, "h2").text.strip()
    except:
        subcategoria = f"Subcategoria_{i+1}"
        
    try:
        link_sub = section.find_element(By.CSS_SELECTOR, "a.dynamic__carousel-link").get_attribute("href")
    except:
        continue

    print(f"\n🟦 Processando: {subcategoria}")

    driver.get(link_sub)
    time.sleep(1)

    # Aguardar produtos carregarem
    try:
        produtos = WebDriverWait(driver, 8).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.poly-component__title"))
        )
        # ADICIONAR ESTA LINHA PARA PRINTAR QUANTOS PRODUTOS:
        print(f"   📊 {len(produtos)} produtos encontrados nesta categoria")
    except:
        print(f"   ⚠ Nenhum produto encontrado")
        continue

    # =======================================================
    # LOOP POR TODOS OS PRODUTOS
    # =======================================================
    for posicao, prod in enumerate(produtos, 1):
        nome = prod.text.strip()
        if not nome:
            continue
            
        link = encurtar_link(prod.get_attribute("href"))
        if not link or not link.startswith("http"):
            continue

        # Abrir produto em nova aba
        driver.execute_script(f"window.open('{link}');")
        driver.switch_to.window(driver.window_handles[1])
        time.sleep(1)  # REDUZIDO

        # Extrair dados básicos (MAIS RÁPIDO - sem prints)
        try:
            preco = extrair_preco_avista(driver)
        except:
            preco = "Preço não identificado"
        
        asin = extrair_asin(link)
        
        try:
            nota_geral, qtd_avaliacoes = extrair_nota_geral_e_avaliacoes(driver)
        except:
            nota_geral, qtd_avaliacoes = "N/A", "N/A"

        
        # ======================================================
        # COLETAR COMENTÁRIOS — VIA CLICK NAS ESTRELAS ⭐⭐⭐⭐⭐
        # ======================================================

        comentarios = []
        comentarios_coletados = 0

        # 1️⃣ Clicar nas estrelas (abre direto o modal correto)
        try:
            estrelas = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a.ui-pdp-review__label"))
            )
            driver.execute_script("arguments[0].click();", estrelas)
            print("Avaliações clicadas (estrelas)")
        except:
            print("   ⚠ Não foi possível clicar nas estrelas")

        time.sleep(2)

       # Detectar iframe (se existir)
        iframe_encontrado = False
        iframes = driver.find_elements(By.TAG_NAME, "iframe")

        for iframe in iframes:
            src = iframe.get_attribute("src") or ""
            if "reviews" in src or "opini" in src:
                driver.switch_to.frame(iframe)
                iframe_encontrado = True
                print("✅ Avaliações renderizadas via iframe")
                break

        if not iframe_encontrado:
            print("✅ Avaliações renderizadas direto no DOM (sem iframe)")

        # 3️⃣ Aguardar seção principal
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "section[data-testid='reviews-desktop']"))
            )
        except:
            print("   ⚠ Seção de reviews não carregou")

        # 4️⃣ Scroll para carregar comentários
        for _ in range(8):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1)

        # 5️⃣ Coletar até 15 comentários
        comentarios_elementos = driver.find_elements(
            By.CSS_SELECTOR, "article.ui-review-capability-comments__comment"
        )

        for comentario_el in comentarios_elementos[:15]:
            texto = extrair_texto_comentario(comentario_el)
            nota = extrair_nota_individual(comentario_el)
            data_comentario = extrair_data_comentario(comentario_el)

            comentarios.append({
                "Comentário": texto,
                "Nota": nota,
                "Data Comentário": data_comentario
            })
            comentarios_coletados += 1

        print(f"✔ Coletados {comentarios_coletados} comentários")

        driver.switch_to.default_content()



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
                "Nota Comentário": comentario["Nota"],
                "Data Comentário": comentario["Data Comentário"],
                "Comentário": comentario["Comentário"],
                "Link": link
            })

        
        # ADICIONAR PRINT FINAL DO PRODUTO:
        print(f"   ✔ Produto {posicao}/{len(produtos)} finalizado - {comentarios_coletados} comentários")

        posicao_global += 1

        # Fechar aba do produto e voltar para lista - MAIS RÁPIDO
        driver.close()
        driver.switch_to.window(driver.window_handles[0])
        time.sleep(0.3)

    # Fechar a aba atual e abrir nova para próxima categoria
    if len(driver.window_handles) > 1:
        driver.close()
        driver.switch_to.window(driver.window_handles[0])

# Salvar resultados
if dados:
    df = pd.DataFrame(dados)
    os.makedirs("resultados", exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-d_%H-%M")
    
    # Arquivo 1: FULL (Todos os comentários)
    file_full = f"resultados/mais_vendidos_ml_FULL_{ts}.csv"
    df.to_csv(file_full, index=False, encoding="utf-8-sig", sep=";")
    
    # Arquivo 2: RESUMO (Sem duplicatas de ASIN, apenas 1 linha por produto)
    file_resumo = f"resultados/mais_vendidos_ml_SAMPLE_{ts}.csv"
    
    # Criar resumo com apenas os dados básicos (primeiro comentário de cada produto)
    df_resumo = df.drop_duplicates(subset=['ASIN', 'Nome'], keep='first')
    df_resumo.to_csv(file_resumo, index=False, encoding="utf-8-sig", sep=";")

    print(f"\n🎉 Relatórios gerados:")
    print(f"1. {file_full} - {len(df)} registros")
    print(f"2. {file_resumo} - {len(df_resumo)} produtos únicos")
else:
    print("⚠️ Nenhum dado coletado.")

driver.quit()