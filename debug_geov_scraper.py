import requests
from bs4 import BeautifulSoup
import json

print("🔧 Debugando problema do GeovRodri...")

# Vamos simular o que o pacote provavelmente faz internamente
url = "https://www.zapimoveis.com.br/aluguel/apartamentos/sp+campinas/"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
}

print(f"🌐 Testando URL: {url}")

try:
    response = requests.get(url, headers=headers, timeout=10)
    
    print(f"📊 Status Code: {response.status_code}")
    print(f"📝 Content-Type: {response.headers.get('content-type', 'N/A')}")
    print(f"📏 Response Length: {len(response.text)}")
    
    # Verificar se é JSON ou HTML
    try:
        json_data = response.json()
        print("✅ Resposta é JSON válido")
    except:
        print("❌ Resposta NÃO é JSON - provavelmente HTML")
        
        # Salvar resposta para análise
        with open('debug_response.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        print("💾 Resposta salva em 'debug_response.html'")
        
        # Mostrar início da resposta
        print(f"\n📄 Primeiros 300 caracteres:")
        print(response.text[:300])
        
        # Verificar se tem indicadores de bloqueio
        if "cloudflare" in response.text.lower():
            print("🛡️ DETECTADO: Proteção Cloudflare")
        elif "blocked" in response.text.lower():
            print("🛡️ DETECTADO: Possível bloqueio")
        elif "403" in response.text:
            print("🛡️ DETECTADO: Erro 403 na página")
        elif "captcha" in response.text.lower():
            print("🤖 DETECTADO: CAPTCHA requerido")
            
except Exception as e:
    print(f"❌ Erro na requisição: {e}")

print("\n🎯 Debug concluído!")