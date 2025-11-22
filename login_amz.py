from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pickle

COOKIES_PATH = "cookies_amazon.pkl"

def fazer_login_e_salvar_cookies():
    options = Options()
    # options.add_argument("--headless")  # descomente se quiser rodar invisível
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    driver.get("https://www.amazon.com.br/ap/signin")

    print("⚠️ Faça login manualmente na aba aberta.")
    input("✅ Após concluir o login, pressione ENTER aqui.")
    
    # Salvar cookies
    with open(COOKIES_PATH, "wb") as file:
        pickle.dump(driver.get_cookies(), file)

    print("✅ Cookies salvos com sucesso em 'cookies_amazon.pkl'")
    driver.quit()

if __name__ == "__main__":
    fazer_login_e_salvar_cookies()
