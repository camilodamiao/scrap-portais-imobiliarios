import requests
import json
import uuid
from pprint import pprint

# Gerar novo device ID
new_device_id = str(uuid.uuid4())
print(f"🆔 Novo Device ID: {new_device_id}")

url = "https://glue-api.zapimoveis.com.br/v2/listings"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
    'Origin': 'https://www.zapimoveis.com.br',
    'Referer': 'https://www.zapimoveis.com.br/',
    'x-deviceid': new_device_id,
    'x-domain': '.zapimoveis.com.br'
}

# Parâmetros mínimos
params = {
    'user': new_device_id,
    'portal': 'ZAP',
    'business': 'RENTAL',
    'size': 3,
    'from': 0
}

print("🔍 Testando com novo Device ID...")

try:
    response = requests.get(url, headers=headers, params=params, timeout=15)
    print(f"📊 Status: {response.status_code}")
    
    if response.status_code == 200:
        print("✅ FUNCIONOU com novo Device ID!")
        data = response.json()
        
        with open('zap_success_v2.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print("💾 Dados salvos em 'zap_success_v2.json'")
        
    else:
        print(f"❌ Status: {response.status_code}")
        print(f"Response: {response.text[:200]}...")
        
except Exception as e:
    print(f"❌ Erro: {e}")