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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import random
from selenium_stealth import stealth

# Tenta definir o formato de datas para português. Se não funcionar, ignora o erro.
try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
except:
    pass 

# Define o caminho dos cookies salvos
COOKIES_PATH = "cookies_amazon.pkl"
URL_MAIS_VENDIDOS = "https://www.amazon.com.br/gp/bestsellers"
MAX_TENTATIVAS = 3
DELAY_BASE = 2
DELAY_ALEATORIO = 3
REINICIAR_A_CADA = 30  # Produtos



def carregar_cookies(driver):
    if os.path.exists(COOKIES_PATH):
        with open(COOKIES_PATH, "rb") as file:
            cookies = pickle.load(file)
            for cookie in cookies:
                if "sameSite" in cookie:
                    cookie.pop("sameSite")
                driver.add_cookie(cookie)
        return True
    return False

def formatar_data_e_pais_amazon(data_str):
    try:
        # Caso 1: Já está no formato DD/MM/YYYY
        if re.match(r'\d{2}/\d{2}/\d{4}', data_str):
            return "Brasil", data_str
        
        # Caso 2: Formato "Avaliado no Brasil em 18 de junho de 2023"
        if 'Brasil' in data_str:
            # Extrair a parte da data
            match = re.search(r'em (.+)', data_str)
            if match:
                data_pt = match.group(1).strip()
                
                # Mapear meses em português
                meses_pt = {
                    'janeiro': '01', 'fevereiro': '02', 'março': '03',
                    'abril': '04', 'maio': '05', 'junho': '06',
                    'julho': '07', 'agosto': '08', 'setembro': '09',
                    'outubro': '10', 'novembro': '11', 'dezembro': '12'
                }
                
                # Tentar parsear "18 de junho de 2023"
                for mes_pt, mes_num in meses_pt.items():
                    if mes_pt in data_pt.lower():
                        # Extrair dia e ano
                        partes = data_pt.split(' de ')
                        if len(partes) >= 3:
                            dia = partes[0].zfill(2)
                            ano = partes[2]
                            return "Brasil", f"{dia}/{mes_num}/{ano}"
                        break
            
            # Se não conseguir parsear, retorna o texto original
            return "Brasil", data_str
        
        # Caso 3: Formato em inglês "Reviewed in the United States on June 18, 2023"
        elif 'United States' in data_str:
            match = re.search(r'on (.+)', data_str)
            if match:
                try:
                    data_en = match.group(1).strip()
                    # Converter para datetime
                    data_obj = datetime.strptime(data_en, "%B %d, %Y")
                    return "EUA", data_obj.strftime('%d/%m/%Y')
                except:
                    return "EUA", data_en
        
        # Caso 4: Outros formatos
        return "N/A", data_str
        
    except Exception as e:
        return "N/A", data_str
    

# DEF REINICIAR DRIVER
def verificar_e_reiniciar_driver(driver, produto_atual, contador_reinicios):
    """Reinicia o driver se atingiu limite"""
    if produto_atual % REINICIAR_A_CADA == 0:
        print(f"   🔄 [{contador_reinicios+1}] Reiniciando driver...")
        try:
            driver.quit()
        except:
            pass
        
        time.sleep(random.uniform(5, 8))
        
        options = Options()
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-notifications")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        novo_driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), 
            options=options
        )
        
        novo_driver.get("https://www.amazon.com.br")
        carregar_cookies(novo_driver)
        time.sleep(2)
        novo_driver.refresh()
        
        return novo_driver, contador_reinicios + 1
    return driver, contador_reinicios


def filtrar_comentario_brasileiro(data_str):
    """
    Verifica se o comentário é do Brasil
    """
    return 'Brasil' in data_str

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
        return "N/A"
    
def extrair_caracteristicas_produto(soup):
    """
    Extrai características importantes do produto como voltagem, capacidade, cor, etc.
    Retorna uma string formatada com as características encontradas.
    """
    caracteristicas = []
    
    try:
        # ============================================
        # 1. EXTRAIR VARIAÇÕES DO PRODUTO (NOVO - da seção twister)
        # ============================================
        variacoes = extrair_variacoes_produto(soup)
        if variacoes != "Sem variações detectadas":
            caracteristicas.append(variacoes)
        
        # ============================================
        # 2. BUSCAR NA TABELA DE DETALHES TÉCNICOS
        # ============================================
        tabela_tecnica = soup.find('table', {'id': 'productDetails_techSpec_section_1'})
        
        if not tabela_tecnica:
            tabela_tecnica = soup.find('table', {'id': 'productDetails_detailBullets_sections1'})
        
        if not tabela_tecnica:
            # Tentar outras tabelas de detalhes
            tabelas = soup.find_all('table', class_='a-keyvalue')
            for tabela in tabelas:
                if 'Detalhes técnicos' in str(tabela) or 'Technical Details' in str(tabela):
                    tabela_tecnica = tabela
                    break
        
        if tabela_tecnica:
            linhas = tabela_tecnica.find_all('tr')
            caracteristicas_tecnicas = []
            
            for linha in linhas:
                try:
                    th = linha.find('th')
                    td = linha.find('td')
                    
                    if th and td:
                        chave = th.get_text(strip=True)
                        valor = td.get_text(strip=True)
                        
                        # Filtrar características importantes
                        chave_lower = chave.lower()
                        if any(palavra in chave_lower for palavra in ['voltagem', 'tensão', 'voltage', 'volts', 'v']):
                            caracteristicas_tecnicas.append(f"Voltagem: {valor}")
                        elif any(palavra in chave_lower for palavra in ['cor', 'color', 'cores']):
                            caracteristicas_tecnicas.append(f"Cor: {valor}")
                        elif any(palavra in chave_lower for palavra in ['capacidade', 'capacity', 'armazenamento', 'storage', 'gb', 'tb']):
                            caracteristicas_tecnicas.append(f"Capacidade: {valor}")
                        elif any(palavra in chave_lower for palavra in ['memória', 'memory', 'ram']):
                            caracteristicas_tecnicas.append(f"Memória: {valor}")
                        elif any(palavra in chave_lower for palavra in ['tela', 'screen', 'display']):
                            caracteristicas_tecnicas.append(f"Tela: {valor}")
                        elif any(palavra in chave_lower for palavra in ['processador', 'processor', 'cpu']):
                            caracteristicas_tecnicas.append(f"Processador: {valor}")
                        elif any(palavra in chave_lower for palavra in ['sistema', 'system', 'os', 'android', 'ios']):
                            caracteristicas_tecnicas.append(f"Sistema: {valor}")
                        elif any(palavra in chave_lower for palavra in ['marca', 'brand']):
                            caracteristicas_tecnicas.append(f"Marca: {valor}")
                        elif any(palavra in chave_lower for palavra in ['modelo', 'model']):
                            caracteristicas_tecnicas.append(f"Modelo: {valor}")
                        elif any(palavra in chave_lower for palavra in ['dimensões', 'dimensions', 'tamanho', 'size']):
                            caracteristicas_tecnicas.append(f"Dimensões: {valor}")
                except:
                    continue
            
            # Adicionar até 3 características técnicas
            if caracteristicas_tecnicas:
                caracteristicas.extend(caracteristicas_tecnicas[:3])
        
        # ============================================
        # 3. BUSCAR NA LISTA DE BULLET POINTS (apenas se precisar)
        # ============================================
        if len(caracteristicas) < 2:  # Se ainda não tem muitas características
            bullet_points = soup.select('#feature-bullets ul.a-unordered-list li')
            if not bullet_points:
                bullet_points = soup.select('.a-unordered-list.a-vertical.a-spacing-mini li')
            
            for bullet in bullet_points[:2]:  # Pegar apenas os primeiros 2
                texto = bullet.get_text(strip=True)
                if len(texto) > 10 and len(texto) < 150:
                    # Verificar se é uma característica relevante
                    texto_lower = texto.lower()
                    if any(termo in texto_lower for termo in ['voltagem', 'cor', 'capacidade', 'memória', 'tela', 'processador']):
                        caracteristicas.append(texto)
        
    except Exception as e:
        print(f"      ⚠ Erro ao extrair características: {str(e)[:50]}")
    
    # ============================================
    # 4. FORMATAR RESULTADO
    # ============================================
    if caracteristicas:
        # Limitar a um total de 4 características
        caracteristicas_final = caracteristicas[:4]
        return " | ".join(caracteristicas_final)
    else:
        return "Não possui características detectáveis"
    
def extrair_variacoes_produto(soup):
    """
    Extrai as variações disponíveis do produto (cor, voltagem, tamanho, etc.)
    especialmente da seção 'twister' da Amazon.
    """
    variacoes = []
    
    try:
        # ============================================
        # 1. BUSCAR NA SEÇÃO TWISTER PRINCIPAL (novo método)
        # ============================================
        twister_section = soup.find('div', {'id': 'twister-plus-inline-twister'})
        
        if twister_section:
            # Buscar todas as linhas de variação dentro do twister
            linhas_variacao = twister_section.find_all('div', class_='inline-twister-row')
            
            for linha in linhas_variacao:
                try:
                    # Extrair o título da variação (ex: "Voltagem", "Cor")
                    titulo_div = linha.find('div', class_='dimension-heading')
                    titulo = ""
                    
                    if titulo_div:
                        titulo_text = titulo_div.get_text(strip=True)
                        # Limpar o título - remover "selecionada é"
                        titulo = titulo_text.split('selecionada é')[0].strip()
                        titulo = titulo.replace(':', '').strip()
                        if not titulo:
                            # Tentar extrair do aria-label
                            aria_label = titulo_div.get('aria-label', '')
                            if 'selecionada é' in aria_label:
                                titulo = aria_label.split('selecionada é')[0].strip()
                    
                    # Se não encontrou título, tentar adivinhar pelo ID
                    if not titulo:
                        linha_id = linha.get('id', '')
                        if 'color' in linha_id:
                            titulo = 'Cor'
                        elif 'voltage' in linha_id or 'voltagem' in linha_id:
                            titulo = 'Voltagem'
                        elif 'size' in linha_id or 'tamanho' in linha_id:
                            titulo = 'Tamanho'
                        else:
                            titulo = 'Variação'
                    
                    # Extrair opções disponíveis
                    opcoes = []
                    
                    # Método 1: Buscar botões de seleção
                    botoes = linha.find_all('button', class_='twister-button')
                    
                    # Método 2: Se não encontrar botões, buscar spans/divs com opções
                    if not botoes:
                        botoes = linha.find_all('li', class_='swatchAvailable')
                    
                    if not botoes:
                        # Método 3: Buscar elementos com data-value
                        elementos_opcao = linha.find_all(['div', 'span', 'li'], attrs={'data-value': True})
                        for elem in elementos_opcao:
                            texto = elem.get_text(strip=True)
                            if texto and texto not in opcoes:
                                opcoes.append(texto)
                    
                    # Extrair texto dos botões
                    for botao in botoes:
                        # Tentar pegar texto de várias formas
                        texto = botao.get_text(strip=True)
                        if not texto or texto == '':
                            # Tentar pegar do aria-label
                            texto = botao.get('aria-label', '')
                            if 'selecionada' in texto:
                                continue  # Pular opção já selecionada
                        
                        if texto and texto not in opcoes and len(texto) < 50:
                            opcoes.append(texto)
                    
                    # Se ainda não encontrou opções, tentar extrair do conteúdo expandido
                    if not opcoes:
                        expander = linha.find('div', class_='dimension-expander-content')
                        if expander:
                            spans = expander.find_all('span')
                            for span in spans:
                                texto = span.get_text(strip=True)
                                if texto and texto not in opcoes:
                                    opcoes.append(texto)
                    
                    # Formatar a variação
                    if titulo and opcoes:
                        # Limitar a 5 opções no máximo
                        opcoes_limpias = []
                        for opcao in opcoes[:5]:
                            # Remover texto desnecessário
                            opcao_limpa = opcao.replace('selecionada', '').replace('selected', '').strip()
                            if opcao_limpa:
                                opcoes_limpias.append(opcao_limpa)
                        
                        if opcoes_limpias:
                            variacoes.append(f"{titulo}: {', '.join(opcoes_limpias)}")
                            
                except Exception as e:
                    continue
        
        # ============================================
        # 2. MÉTODO ALTERNATIVO: BUSCAR POR ATRIBUTOS COMUNS
        # ============================================
        if not variacoes:
            # Procurar por elementos que contêm "Voltagem" ou "Cor"
            textos_variacao = soup.find_all(string=re.compile(r'(Voltagem|Cor|Tamanho|Tamanho do|Capacidade)', re.IGNORECASE))
            
            for texto in textos_variacao:
                try:
                    elemento_pai = texto.parent
                    if elemento_pai:
                        # Procurar opções próximas
                        proximos_elementos = elemento_pai.find_next_siblings(['div', 'span', 'ul'])
                        
                        opcoes = []
                        titulo = texto.strip().replace(':', '')
                        
                        for elem in proximos_elementos[:3]:  # Olhar apenas os 3 primeiros
                            if elem.name == 'ul':
                                # Lista de opções
                                itens = elem.find_all('li')
                                for item in itens:
                                    opcao = item.get_text(strip=True)
                                    if opcao and len(opcao) < 30:
                                        opcoes.append(opcao)
                            else:
                                # Texto direto
                                opcao = elem.get_text(strip=True)
                                if opcao and len(opcao) < 30 and opcao != titulo:
                                    opcoes.append(opcao)
                        
                        if titulo and opcoes:
                            variacoes.append(f"{titulo}: {', '.join(opcoes[:3])}")
                            
                except:
                    continue
        
        # ============================================
        # 3. MÉTODO DIRETO: BUSCAR POR VALORES ESPECÍFICOS
        # ============================================
        if not variacoes:
            # Buscar especificamente por "110 Volts" e "220 Volts"
            texto_pagina = soup.get_text()
            
            # Verificar voltagens
            if '110' in texto_pagina and 'Volts' in texto_pagina:
                voltagens = []
                if '110 Volts' in texto_pagina:
                    voltagens.append('110 Volts')
                if '220 Volts' in texto_pagina:
                    voltagens.append('220 Volts')
                if '127 Volts' in texto_pagina:
                    voltagens.append('127 Volts')
                
                if voltagens:
                    variacoes.append(f"Voltagem: {', '.join(voltagens)}")
            
            # Verificar cores comuns
            cores_comuns = ['preto', 'branco', 'vermelho', 'azul', 'cinza', 'prata', 'dourado']
            cores_encontradas = []
            
            for cor in cores_comuns:
                if cor in texto_pagina.lower():
                    cores_encontradas.append(cor.capitalize())
            
            if cores_encontradas:
                variacoes.append(f"Cor: {', '.join(cores_encontradas[:3])}")
        
    except Exception as e:
        print(f"      ⚠ Erro ao extrair variações: {str(e)[:50]}")
    
    # ============================================
    # 4. FORMATAR RESULTADO
    # ============================================
    if variacoes:
        # Juntar todas as variações (máximo 3)
        return " | ".join(variacoes[:3])
    else:
        return "Sem variações detectadas"
    
def extrair_detalhes_produto(driver, asin, posicao_global, subcategoria, posicao_categoria):
    """Extrai detalhes do produto COM RETRY"""
    
    for tentativa in range(MAX_TENTATIVAS):
        try:
            url = f"https://www.amazon.com.br/dp/{asin}"
            driver.get(url)
            
            # DELAY ALEATÓRIO
            time.sleep(DELAY_BASE + random.uniform(0.5, 2.5))
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # VERIFICAÇÃO DE PÁGINA VÁZIA - CORRIGIR INDENTAÇÃO AQUI
            if len(soup.text) < 100 or "não encontramos" in soup.text.lower():
                if tentativa < MAX_TENTATIVAS - 1:
                    time.sleep(3)
                    continue
                else:
                    return "Produto não disponível", "N/A", "N/A", "N/A", "N/A"

            # DEBUG: Verificar se é Apps e Jogos pela URL ou título
            is_apps_jogos = any(termo in driver.current_url.lower() for termo in ['/dp/', '/gp/', 'apps', 'jogos'])
            
            # ============================================
            # 1. EXTRAIR NOME DO PRODUTO (com fallbacks)
            # ============================================
            nome_produto = "Nome não encontrado"
            
            nome_produto_tag = soup.select_one('#productTitle')
            if nome_produto_tag:
                nome_produto = nome_produto_tag.get_text(strip=True)
            else:
                nome_tag_fallbacks = [
                    'h1.a-size-large',
                    '#title',
                    'span.a-size-extra-large',
                    'div#title_feature_div h1',
                    'div#titleSection h1'
                ]
                
                for selector in nome_tag_fallbacks:
                    tag = soup.select_one(selector)
                    if tag:
                        nome_produto = tag.get_text(strip=True)
                        break
            
            # ============================================
            # 2. EXTRAIR PREÇO (com fallbacks)
            # ============================================
            preco = "Preço não disponível"
            
            preco_tag = soup.select_one('span.a-price span.a-offscreen')
            
            if preco_tag:
                preco_text = preco_tag.get_text(strip=True)
                preco = preco_text.replace("R$", "").replace(".", "").strip()
            else:
                preco_fallbacks = [
                    'span.a-price-whole',
                    'span.offer-price',
                    'div.a-section.a-spacing-none.apexPriceToPay span.a-offscreen',
                    'td.a-span12 span.a-color-price'
                ]
                
                for selector in preco_fallbacks:
                    tag = soup.select_one(selector)
                    if tag:
                        preco_text = tag.get_text(strip=True)
                        if 'R$' in preco_text:
                            preco = preco_text.replace("R$", "").replace(".", "").strip()
                        else:
                            preco = ''.join(filter(str.isdigit, preco_text))
                        break
            
            # ============================================
            # 3. EXTRAIR NOTA GERAL
            # ============================================
            nota_geral_tag = soup.select_one('#averageCustomerReviews span.a-icon-alt')
            if not nota_geral_tag:
                nota_geral_tag = soup.select_one('span[data-hook="rating-out-of-text"]')
            
            nota_geral = nota_geral_tag.get_text(strip=True).split()[0].replace(".", ",") if nota_geral_tag else "N/A"
            
            # ============================================
            # 4. EXTRAIR QUANTIDADE DE AVALIAÇÕES
            # ============================================
            qtd_avaliacoes = extrair_qtd_avaliacoes(soup)
            
            # ============================================
            # 5. EXTRAIR CARACTERÍSTICAS DO PRODUTO
            # ============================================
            caracteristicas = extrair_caracteristicas_produto(soup)
            
            # DEBUG: Print para verificar extração
            print(f"      📝 Nome: {nome_produto[:50]}...")
            print(f"      💰 Preço: {preco}")
            print(f"      ⭐ Nota: {nota_geral}")
            print(f"      📊 Avaliações: {qtd_avaliacoes}")
            print(f"      🔧 Características: {caracteristicas[:100]}...")
            
            return nome_produto, preco, nota_geral, qtd_avaliacoes, caracteristicas
            
        except Exception as e:
            print(f"      ⚠ Tentativa {tentativa+1} falhou: {str(e)[:50]}")
            if tentativa < MAX_TENTATIVAS - 1:
                time.sleep(random.uniform(3, 6))
                continue
    
    return "Erro na extração", "N/A", "N/A", "N/A", "N/A"

def extrair_comentarios_amazon(driver, asin, max_comentarios=15):
    comentarios = []
    try:
        url = f"https://www.amazon.com.br/product-reviews/{asin}/"
        driver.get(url)
        time.sleep(3)
        
        # Tentar aplicar filtro de Brasil
        try:
            # Procurar por filtro de localização
            time.sleep(1)
            filtros = driver.find_elements(By.CSS_SELECTOR, "a[data-hook='cr-filter']")
            for filtro in filtros:
                if "Brasil" in filtro.text:
                    driver.execute_script("arguments[0].click();", filtro)
                    time.sleep(2)
                    break
        except:
            pass
        
        # Processar comentários
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        blocos = soup.find_all("li", {"data-hook": "review"})
        
        if not blocos:
            blocos = soup.find_all("div", id=lambda x: x and x.startswith("customer_review-"))
        
        if not blocos:
            return comentarios
        
        # Coletar APENAS comentários brasileiros
        for bloco in blocos:
            try:
                # Extrair data/localização PRIMEIRO para filtrar
                data_tag = bloco.select_one("span[data-hook='review-date']")
                data_str = data_tag.get_text(strip=True) if data_tag else ""
                
                # FILTRAR: Pular comentários que NÃO são do Brasil
                if not filtrar_comentario_brasileiro(data_str):
                    continue  # ⬅️ IGNORA comentários de outros países
                
                # Extrair nota
                nota_tag = bloco.select_one("i[data-hook='review-star-rating'] span")
                nota = nota_tag.get_text(strip=True).split()[0].replace(".", ",") if nota_tag else "N/A"
                
                # Extrair texto
                texto_container = bloco.select_one("span[data-hook='review-body'] span")
                texto = texto_container.get_text(strip=True) if texto_container else "Sem texto"
                
                # Formatar data (mantém país como Brasil)
                pais = "Brasil"
                data_formatada = formatar_data_e_pais_amazon(data_str)[1] if data_str else "N/D"
                
                comentarios.append({
                    "Nota Comentário": nota,
                    "País": pais,
                    "Data Comentário": data_formatada,
                    "Comentário": texto
                })
                
                # Parar quando atingir o limite
                if len(comentarios) >= max_comentarios:
                    break
                    
            except Exception as e:
                continue
        
    except Exception as e:
        print(f"   ⚡ Erro na coleta de comentários: {str(e)[:50]}")
    
    return comentarios

def extrair_subcategorias_amazon(driver):
    """Extrai todas as subcategorias da página de mais vendidos da Amazon"""
    driver.get(URL_MAIS_VENDIDOS)
    time.sleep(5)  # Aumentei de 3 para 5 segundos
    
    subcategorias = []

    try:
        # Primeiro, tente com wait explícito
        wait = WebDriverWait(driver, 10)
        
        # TENTE ESTES SELETORES ALTERNATIVOS (um por um):
        
        # Opção 1: Pelo ID original (se existir)
        try:
            menu = wait.until(EC.presence_of_element_located((By.ID, "zg_browseRoot")))
        except:
            # Opção 2: Pela classe do menu lateral
            menu = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#zg-left-col ul, div[role='navigation'] ul, .a-unordered-list")
            ))
        
        # Encontrar todos os links relevantes
        # Usar XPath para pegar apenas links dentro do menu de navegação
        links = menu.find_elements(By.XPATH, ".//a[contains(@href, '/gp/bestsellers/')]")
        
        if not links:
            # Alternativa: buscar todos os links da coluna esquerda
            links = driver.find_elements(By.CSS_SELECTOR, "#zg-left-col a")
        
        for link in links:
            nome = link.text.strip()
            href = link.get_attribute("href")
            
            # Filtros mais precisos
            if (nome and 
                href and 
                "/gp/bestsellers/" in href and
                nome not in ["Página inicial", "Home", "Voltar", "Ver todos"] and
                len(nome) > 2):  # Ignorar textos muito curtos
                
                subcategorias.append({
                    "nome": nome,
                    "link": href
                })
        
        print(f"✅ Encontradas {len(subcategorias)} subcategorias válidas")
        
    except Exception as e:
        print(f"❌ Erro ao buscar subcategorias: {e}")
        
        # FALLBACK: Tentar método alternativo se o menu não for encontrado
        try:
            print("🔄 Tentando método alternativo...")
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Buscar todos os links que parecem ser categorias
            all_links = soup.find_all('a', href=lambda x: x and '/gp/bestsellers/' in x)
            
            for link in all_links:
                nome = link.text.strip()
                href = link.get('href')
                if nome and href and nome not in ["", "Página inicial"]:
                    # Converter URL relativa para absoluta se necessário
                    if not href.startswith('http'):
                        href = "https://www.amazon.com.br" + href
                    
                    subcategorias.append({
                        "nome": nome,
                        "link": href
                    })
            
            # Remover duplicatas
            subcategorias = [dict(t) for t in {tuple(d.items()) for d in subcategorias}]
            print(f"✅ Método alternativo encontrou {len(subcategorias)} subcategorias")
            
        except Exception as e2:
            print(f"❌ Método alternativo também falhou: {e2}")
    
    return subcategorias

def extrair_subcategoria_pela_pagina(driver):
    """
    Extrai o nome real da subcategoria a partir do título
    'Mais Vendidos em X'
    """
    try:
        # Aguardar carregamento
        time.sleep(2)
        
        # Método 1: Do h1 principal
        try:
            h1 = driver.find_element(By.TAG_NAME, "h1")
            h1_text = h1.text.strip()
            if h1_text and "Mais Vendidos em" in h1_text:
                categoria = h1_text.replace("Mais Vendidos em", "").strip()
                print(f"   📍 Subcategoria do h1: {categoria}")
                return categoria
        except:
            pass
        
        # Método 2: Do título da div com a classe específica
        try:
            div_titulo = driver.find_element(By.CSS_SELECTOR, "div._cDEzb_card-title_2sYgw h1")
            titulo_text = div_titulo.text.strip()
            if titulo_text and "Mais Vendidos em" in titulo_text:
                categoria = titulo_text.replace("Mais Vendidos em", "").strip()
                print(f"   📍 Subcategoria do card: {categoria}")
                return categoria
        except:
            pass
        
        # Método 3: Extrair da URL
        url = driver.current_url
        if "/gp/bestsellers/" in url:
            # Exemplo: https://www.amazon.com.br/gp/bestsellers/grocery/
            categoria_url = url.split("/gp/bestsellers/")[1].split("/")[0]
            
            # Mapear URLs para nomes legíveis
            categoria_map = {
                "grocery": "Alimentos e Bebidas",
                "kitchen": "Cozinha",
                "electronics": "Eletrônicos",
                "books": "Livros",
                # Adicione mais mapeamentos conforme necessário
            }
            
            if categoria_url in categoria_map:
                categoria = categoria_map[categoria_url]
            else:
                categoria = categoria_url.replace("-", " ").title()
            
            print(f"   📍 Subcategoria da URL: {categoria}")
            return categoria
        
        print("   ⚠ Não foi possível detectar a subcategoria, usando padrão")
        return "Mais Vendidos"
        
    except Exception as e:
        print(f"   ❌ Erro ao detectar subcategoria: {e}")
        return "Mais Vendidos"


def extrair_produtos_da_subcategoria(driver, link_subcategoria):
    """Extrai todos os produtos de uma subcategoria na ordem correta (#1, #2, #3...)"""
    driver.get(link_subcategoria)
    time.sleep(5)
    
    produtos = []
    
    try:
        # Aguardar carregamento da lista de produtos
        wait = WebDriverWait(driver, 10)
        
        # Método 1: Buscar pela lista ordenada de produtos
        try:
            lista_produtos = wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "ol.a-ordered-list.a-vertical.p13n-gridRow")
                )
            )
            
            # Encontrar todos os itens da lista
            itens = lista_produtos.find_elements(By.CSS_SELECTOR, "li.zg-no-numbers")
            
            for item in itens:
                try:
                    # Extrair ASIN do atributo data-asin
                    asin = item.get_attribute("data-asin")
                    if asin and len(asin) == 10:
                        produtos.append(asin)
                        continue
                    
                    # Se não encontrar no data-asin, procurar no link do produto
                    link_element = item.find_element(By.CSS_SELECTOR, "a[href*='/dp/']")
                    if link_element:
                        href = link_element.get_attribute("href")
                        match = re.search(r'/dp/([A-Z0-9]{10})', href)
                        if match:
                            produtos.append(match.group(1))
                except:
                    continue
                    
        except:
            # Método 2: Buscar pelos elementos com data-asin
            elementos_asin = driver.find_elements(By.CSS_SELECTOR, "[data-asin]")
            for elemento in elementos_asin:
                asin = elemento.get_attribute("data-asin")
                if asin and len(asin) == 10 and asin not in produtos:
                    produtos.append(asin)
        
        # Se ainda não encontrou produtos, usar o método antigo como fallback
        if not produtos:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            asins = set()
            
            # Encontrar todos os links de produtos com badges (#1, #2, etc.)
            produtos_elements = soup.select('div.zg-grid-general-faceout')
            
            for produto in produtos_elements:
                link = produto.select_one('a[href*="/dp/"]')
                if link:
                    href = link.get('href', '')
                    match = re.search(r'/dp/([A-Z0-9]{10})', href)
                    if match:
                        asin = match.group(1)
                        asins.add(asin)
            
            produtos = list(asins)
        
        print(f"   ✅ {len(produtos)} produtos encontrados na lista de mais vendidos")
        
    except Exception as e:
        print(f"   ❌ Erro ao extrair produtos: {e}")
        # Fallback: método antigo
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        asins = set()
        
        links_produtos = soup.select('a[href*="/dp/"]')
        
        for link in links_produtos:
            href = link.get('href', '')
            match = re.search(r'/dp/([A-Z0-9]{10})', href)
            if match:
                asin = match.group(1)
                asins.add(asin)
        
        produtos = list(asins)
    
    return produtos

def main():
    if not os.path.exists(COOKIES_PATH):
        print("❌ Cookies não encontrados. Rode 'login_amazon.py' primeiro.")
        return

    # Configuração do Chrome
    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-notifications")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
    # options.add_argument("--headless")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    stealth(driver,
    languages=["pt-BR", "pt"],
    vendor="Google Inc.",
    platform="Win32",
    webgl_vendor="Intel Inc.",
    renderer="Intel Iris OpenGL Engine",
    fix_hairline=True,
)
    contador_reinicios = 0

    try:
        print("🔄 Iniciando navegador e carregando cookies...")
        driver.get("https://www.amazon.com.br")
        carregar_cookies(driver)
        time.sleep(2)
        driver.refresh()
        time.sleep(2)

        print("\n🔍 Buscando todas as subcategorias de mais vendidos...")
        subcategorias = extrair_subcategorias_amazon(driver)
        
        # if not subcategorias:
        #     print("⚠ Nenhuma subcategoria encontrada. Usando URL padrão.")
        #     subcategorias = [{"nome": "Mais Vendidos", "link": URL_MAIS_VENDIDOS}]
        
        print(f"📌 {len(subcategorias)} subcategorias encontradas.")
        # print(f"⚠️  Limitando para apenas 7 categorias para teste...")
        # subcategorias = subcategorias[:7] # Limita a 5 categorias ##################################### LIMITAÇÃO CATEGORIAS
        print(f"📌 {len(subcategorias)} subcategorias encontradas.")

        
        todos_dados = []
        posicao_global = 1
        
        # =======================================================
        # LOOP POR CADA SUBCATEGORIA
        # =======================================================
        for idx, subcat in enumerate(subcategorias, 1):
            subcategoria_link = subcat["link"]
            subcategoria_nome_original = subcat["nome"]  # Nome extraído do menu lateral
            
            print(f"\n" + "="*60)
            print(f"🟦 PROCESSANDO SUBCATEGORIA {idx}/{len(subcategorias)}")
            print(f"   📌 Nome no menu: {subcategoria_nome_original}")
            print(f"   🔗 Link: {subcategoria_link}")
            print("="*60)

            driver.get(subcategoria_link)
            time.sleep(3)
            
            # DEBUG: Print título da página para verificação
            print(f"   🏷️ Título da página: {driver.title}")
            print(f"   🌐 URL atual: {driver.current_url}")
            
            subcategoria_nome = extrair_subcategoria_pela_pagina(driver)
            
            print(f"   📍 Subcategoria detectada: {subcategoria_nome}")
            
            # Extrair produtos desta subcategoria
            asins = extrair_produtos_da_subcategoria(driver, subcategoria_link)
            
            if not asins:
                print(f"   ⚠ Nenhum produto encontrado nesta subcategoria")
                continue
            
            print(f"   📊 {len(asins)} produtos encontrados")
            print(f"   📋 Primeiros 5 ASINs: {asins[:5]}")
            
            # Limitar para teste (remova depois de testar)
            # asins = asins[:15] # Limita a 15 produtos para teste ##################################### LIMITAÇÃO PRODUTOS
            
            
            # =======================================================
            # LOOP POR TODOS OS PRODUTOS
            # =======================================================
            for posicao_categoria, asin in enumerate(asins, 1):
                try:
                    # ADICIONE ESTAS 5 LINHAS:
                    time.sleep(random.uniform(1, 2.5))
                    
                    if posicao_categoria % 5 == 0:
                        print(f"   ⏸️ Pausa estratégica...")
                        time.sleep(random.uniform(5, 8))
                    
                    driver, contador_reinicios = verificar_e_reiniciar_driver(driver, posicao_categoria, contador_reinicios)

                    print(f"   📦 Processando produto {posicao_categoria}/{len(asins)} (ASIN: {asin})")
                    
                    # Extrair detalhes básicos do produto (agora com características)
                    nome_produto, preco, nota_geral, qtd_avaliacoes, caracteristicas = extrair_detalhes_produto(
                        driver, asin, posicao_global, subcategoria_nome, posicao_categoria
                    )
                    
                    # Coletar comentários (até 15)
                    comentarios = []
                    comentarios_coletados = 0
                    
                    try:
                        comentarios = extrair_comentarios_amazon(driver, asin, max_comentarios=15)
                        comentarios_coletados = len(comentarios)
                        print(f"   ✅ {comentarios_coletados} comentários coletados")
                    except Exception as e:
                        print(f"   ⚡ Erro ao coletar comentários: {str(e)[:50]}")
                    
                    # Se não coletou comentários, adicionar um placeholder
                    if not comentarios:
                        comentarios.append({
                            "Nota Comentário": "N/A",
                            "País": "N/A",
                            "Data Comentário": "N/A",
                            "Comentário": "Sem comentários disponíveis"
                        })
                    
                    # Adicionar cada comentário como linha separada
                    for comentario in comentarios:
                        todos_dados.append({
                            "Posição Global": posicao_global,
                            "Posição Categoria": posicao_categoria,
                            "Subcategoria": subcategoria_nome,
                            "ASIN": asin,
                            "Nome": nome_produto,
                            "Preço": preco,
                            "Nota Geral": nota_geral,
                            "Qtd. Avaliações": qtd_avaliacoes,
                            "Características": caracteristicas,  # ← NOVA COLUNA AQUI
                            "País": comentario["País"],
                            "Data Comentário": comentario["Data Comentário"],
                            "Nota Comentário": comentario["Nota Comentário"],
                            "Comentário": comentario["Comentário"],
                            "Link": f"https://www.amazon.com.br/dp/{asin}"
                        })
                    
                    print(f"   ✔ Produto {posicao_categoria}/{len(asins)} finalizado - {comentarios_coletados} comentários")
                    posicao_global += 1

                except Exception as e:
                    print(f"   ❌ Erro no produto ASIN {asin}: {str(e)[:50]}")
                
                    if "invalid session id" in str(e) or "no such window" in str(e):
                        print("   🔄 Sessão expirada, recriando driver...")
                        driver, contador_reinicios = verificar_e_reiniciar_driver(driver, 1, contador_reinicios)
                    
                    continue
        
        # =======================================================
        # SALVAR RESULTADOS
        # =======================================================
        if todos_dados:
            df = pd.DataFrame(todos_dados)
            os.makedirs("resultados_amazon", exist_ok=True)
            ts = datetime.now().strftime("%Y-%m-d_%H-%M")
            
            # Ordenar colunas
            colunas = [
                "Posição Global", "Posição Categoria", "Subcategoria", "ASIN", 
                "Nome", "Preço", "Nota Geral", "Qtd. Avaliações", "Características", "País",
                "Data Comentário", "Nota Comentário", "Comentário", "Link"
            ]
            
            # Garantir que todas as colunas existam
            for col in colunas:
                if col not in df.columns:
                    df[col] = "N/A"
            
            df = df[colunas]
            
            # Arquivo 1: FULL (Todos os comentários)
            file_full = f"resultados_amazon/mais_vendidos_amazon_FULL_{ts}.csv"
            df.to_csv(file_full, index=False, encoding="utf-8-sig", sep=";")
            
            # Arquivo 2: RESUMO (Apenas 1 linha por produto - primeiro comentário)
            file_resumo = f"resultados_amazon/mais_vendidos_amazon_SAMPLE_{ts}.csv"
            df_resumo = df.drop_duplicates(subset=['ASIN', 'Nome'], keep='first')
            df_resumo.to_csv(file_resumo, index=False, encoding="utf-8-sig", sep=";")

            print(f"\n🎉 Relatórios gerados:")
            print(f"1. {file_full} - {len(df)} registros")
            print(f"2. {file_resumo} - {len(df_resumo)} produtos únicos")
            print(f"\n📊 Estatísticas:")
            print(f"   • Subcategorias processadas: {len(subcategorias)}")
            print(f"   • Produtos únicos coletados: {len(df_resumo)}")
            print(f"   • Total de registros (com comentários): {len(df)}")
        else:
            print("\n⚠️ Nenhum dado coletado. Verifique a conexão ou os seletores.")
            
    except Exception as e:
        print(f"\n❌ Erro durante a execução: {str(e)}")
    finally:
        driver.quit()
        print("\n🛑 Navegador fechado")

if __name__ == "__main__":
    main() 