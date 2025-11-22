from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pandas as pd
from datetime import datetime
import os
import time
from urllib.parse import urlparse
import re
from selenium.webdriver.chrome.service import Service 
from webdriver_manager.chrome import ChromeDriverManager

# Configuração do Chrome
options = Options()
# options.add_argument("--headless")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-notifications")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


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

# ==============================================================================
# FUNÇÃO DE PREÇO MODIFICADA
# ==============================================================================
def extrair_preco_avista(driver):
    try:
        # Tenta pegar o container principal de preços
        price_container = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.ui-pdp-price__main, div.ui-pdp-price__second-line"))
        )
        
        # Extrai o preço principal
        preco_element = price_container.find_element(By.CSS_SELECTOR, "span.andes-money-amount__fraction")
        centavos_element = price_container.find_elements(By.CSS_SELECTOR, "span.andes-money-amount__cents")
        
        preco = preco_element.text.replace(".", "")
        centavos = centavos_element[0].text if centavos_element else "00"
        
        # Retorna apenas o valor numérico formatado
        return f"{preco},{centavos}"
            
    except Exception:
        try:
            # Fallback: tenta qualquer estrutura de preço
            preco_fallback = driver.find_element(By.CSS_SELECTOR, "span.andes-money-amount__fraction").text.replace(".", "")
            centavos_fallback_elements = driver.find_elements(By.CSS_SELECTOR, "span.andes-money-amount__cents")
            centavos_fallback = centavos_fallback_elements[0].text if centavos_fallback_elements else "00"
            return f"{preco_fallback},{centavos_fallback}"
        except:
            return "Preço não identificado"

def extrair_nota(comentario_element):
    try:
        estrelas_cheias = comentario_element.find_elements(By.CSS_SELECTOR, "use[href='#poly_star_fill']")
        nota = len(estrelas_cheias)
        if comentario_element.find_elements(By.CSS_SELECTOR, "use[href='#poly_star_half']"):
            nota += 0.5
        return str(nota) if nota > 0 else "0"
    except:
        return "N/A"

def extrair_asin(link):
    try:
        asin = re.search(r"(MLB-?\d+)", link).group()
        return asin
    except:
        return "N/A"

def extrair_nota_geral_e_avaliacoes(driver):
    try:
        review_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a.ui-pdp-review__label"))
        )
        nota_geral = review_element.find_element(By.CSS_SELECTOR, "span.ui-pdp-review__rating").text
        nota_geral = nota_geral.replace(".", ",") if nota_geral else "N/A"
        qtd_avaliacoes = review_element.find_element(By.CSS_SELECTOR, "span.ui-pdp-review__amount").text
        qtd_avaliacoes = qtd_avaliacoes.replace("(", "").replace(")", "").replace(".", "").strip()
        return nota_geral, qtd_avaliacoes
    except:
        print("⚠ Não foi possível encontrar a nota geral e a quantidade de avaliações.")
        return "N/A", "N/A"

def extrair_data_comentario(comentario_element):
    try:
        data_original = comentario_element.find_element(By.CSS_SELECTOR, "span.ui-review-capability-comments__comment__date").text.strip()
        return formatar_data(data_original)
    except:
        return "N/A"

# URL
url = "https://www.mercadolivre.com.br/mais-vendidos/MLB5726"
print("🔍 Acessando página de mais vendidos em Eletrodomésticos...")
driver.get(url)

time.sleep(3)
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
time.sleep(2)

try:
    produtos = WebDriverWait(driver, 15).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.poly-component__title"))
    )
    print(f"✅ {len(produtos)} produtos encontrados.")
except Exception as e:
    print(f"❌ Erro ao carregar produtos: {e}")
    driver.quit()
    exit()

dados = []
for posicao, prod in enumerate(produtos[:5], 1):  # Limita aos 5 primeiros
    try:
        nome = prod.text.strip()
        link = encurtar_link(prod.get_attribute("href"))

        if not link or not link.startswith("http"):
            print(f"⚠ Produto '{nome}' ignorado (link inválido)")
            continue

        driver.execute_script(f"window.open('{link}');")
        driver.switch_to.window(driver.window_handles[1])
        
        preco = extrair_preco_avista(driver)
        asin = extrair_asin(link)
        nota_geral, qtd_avaliacoes = extrair_nota_geral_e_avaliacoes(driver)
        
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.5);")
        time.sleep(1)
        
        comentarios = []
        try:
            # ETAPA 1: Clicar no botão "Mostrar todas as opiniões" para carregar o iframe.
            ver_mais_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='see-more']"))
            )
            driver.execute_script("arguments[0].click();", ver_mais_button)
            print("✅ Clicou em 'Mostrar todas as opiniões'.")

            # ETAPA 2: Esperar o iframe aparecer e mudar o foco do driver para ele.
            iframe = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "ui-pdp-iframe-reviews"))
            )
            driver.switch_to.frame(iframe)
            print("✅ Entrou no iframe de avaliações.")

            # ETAPA 3: Encontrar o elemento de rolagem DENTRO do iframe.
            scroll_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "section[data-testid='reviews-desktop']"))
            )
            print("🔄 Iniciando rolagem para carregar todos os comentários...")
            
            # ETAPA 4: Loop de rolagem inteligente.
            last_height = 0
            while True:
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scroll_element)
                time.sleep(2)
                new_height = driver.execute_script("return arguments[0].scrollHeight", scroll_element)
                if new_height == last_height:
                    print("✅ Fim da rolagem.")
                    break
                last_height = new_height
            
            # ETAPA 5: Coletar todos os comentários agora que estão visíveis.
            comentarios_elementos = driver.find_elements(By.CSS_SELECTOR, "article.ui-review-capability-comments__comment")
            print(f"📰 Coletando {len(comentarios_elementos)} comentários...")

            for comentario_el in comentarios_elementos:
                try:
                    texto = comentario_el.find_element(By.CSS_SELECTOR, "p.ui-review-capability-comments__comment__content").text.strip()
                    nota = extrair_nota(comentario_el)
                    data_comentario = extrair_data_comentario(comentario_el)
                    comentarios.append({
                        "Comentário": texto, "Nota": nota, "Data Comentário": data_comentario
                    })
                except Exception as e_inner:
                    print(f"⚠ Erro em comentário individual: {str(e_inner)[:100]}")
                    continue
        
        except TimeoutException:
            print("⚠ Botão 'Mostrar todas as opiniões' ou iframe não encontrado. O produto pode não ter comentários.")
        
        finally:
            # ETAPA 6: ESSENCIAL - Voltar para o conteúdo principal da página.
            driver.switch_to.default_content()
            print("✅ Contexto retornado para a página principal.")

        if not comentarios:
            comentarios.append({"Comentário": "Sem comentários disponíveis", "Nota": "N/A", "Data Comentário": "N/A"})

        for comentario in comentarios:
            dados.append({
                "Posição": posicao, "ASIN": asin, "Nota Geral": nota_geral,
                "Qtd. Avaliações": qtd_avaliacoes, "Data Comentário": comentario["Data Comentário"],
                "Nome": nome, "Preço à vista": preco, "Nota": comentario["Nota"],
                "Comentário": comentario["Comentário"], "Link": link
            })

        print(f"✅ {posicao}° - '{nome[:30]}...' | {len(comentarios)} comentários coletados.")

        driver.close()
        driver.switch_to.window(driver.window_handles[0])
        time.sleep(1)

    except Exception as e:
        print(f"❌ Erro GERAL no produto {posicao}°: {e}")
        if len(driver.window_handles) > 1:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])

# Salva os dados
if dados:
    df = pd.DataFrame(dados)
    df = df.sort_values(by="Posição")
    
    os.makedirs("resultados", exist_ok=True)
    agora = datetime.now().strftime("%Y-%m-%d_%H-%M")
    arquivo = f"resultados/mais_vendidos_eletrodomesticos_{agora}.csv"
    
    df.to_csv(arquivo, index=False, encoding="utf-8-sig", sep=";",
              columns=["Posição", "ASIN", "Nota Geral", "Qtd. Avaliações", "Data Comentário", 
                       "Nome", "Preço à vista", "Nota", "Comentário", "Link"])

    print(f"\n📊 Relatório salvo em: {arquivo}")
    print("\n🔍 Amostra dos dados:")
    print(df.head())
else:
    print("⚠ Nenhum dado foi coletado")

driver.quit()
print("🛑 Navegador fechado.")
