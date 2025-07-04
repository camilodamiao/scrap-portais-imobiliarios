import requests
import json
from pprint import pprint

# URL base
url = "https://glue-api.zapimoveis.com.br/v2/listings"

# Headers completos (copiados do seu DevTools)
headers = {
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36',
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Origin': 'https://www.zapimoveis.com.br',
    'Referer': 'https://www.zapimoveis.com.br/',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'x-deviceid': '89c865f0-176f-453a-87c3-68ec2573558c',
    'x-domain': '.zapimoveis.com.br'
}

# Parâmetros EXATOS do seu DevTools (simplificados)
params = {
    'user': '89c865f0-176f-453a-87c3-68ec2573558c',
    'portal': 'ZAP',
    'includeFields': 'search(result(listings(listing(id,title,address,pricingInfos,usableAreas,bedrooms,bathrooms)),totalCount))',
    'categoryPage': 'RESULT',
    'business': 'RENTAL',
    'listingType': 'USED',
    'page': 1,
    'size': 5,  # Reduzido para teste
    'from': 0,
    'images': 'webp'
}

print("🔍 Testando com parâmetros completos...")
print(f"URL: {url}")

try:
    response = requests.get(url, headers=headers, params=params, timeout=15)
    
    print(f"\n📊 Status Code: {response.status_code}")
    
    if response.status_code == 200:
        print("✅ SUCESSO!")
        
        data = response.json()
        print(f"🔢 Tipo: {type(data)}")
        print(f"🗝️ Chaves: {list(data.keys())}")
        
        # Salvar resposta
        with open('zap_success.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print("💾 Resposta salva em 'zap_success.json'")
        
        # Verificar estrutura
        if 'search' in data:
            result = data['search'].get('result', {})
            listings = result.get('listings', [])
            total = result.get('totalCount', 0)
            
            print(f"\n🏠 Total disponível: {total}")
            print(f"🏠 Retornados agora: {len(listings)}")
            
            if listings:
                first = listings[0]['listing']
                print(f"\n📝 Primeira listagem:")
                print(f"   ID: {first.get('id')}")
                print(f"   Título: {first.get('title', '')[:60]}...")
                
                # Preço
                pricing = first.get('pricingInfos', [])
                if pricing:
                    price = pricing[0].get('price', 'N/A')
                    print(f"   Preço: R$ {price:,}" if isinstance(price, (int, float)) else f"   Preço: {price}")
                
                # Endereço
                addr = first.get('address', {})
                city = addr.get('city', '')
                neighborhood = addr.get('neighborhood', '')
                print(f"   Local: {neighborhood}, {city}")
                
                # Detalhes
                area = first.get('usableAreas', [])
                bedrooms = first.get('bedrooms', [])
                bathrooms = first.get('bathrooms', [])
                
                if area: print(f"   Área: {area[0]}m²")
                if bedrooms: print(f"   Quartos: {bedrooms[0]}")
                if bathrooms: print(f"   Banheiros: {bathrooms[0]}")
        
    elif response.status_code == 403:
        print("❌ Ainda bloqueado (403)")
        print("🔧 Vamos tentar uma abordagem diferente...")
        
    else:
        print(f"❌ Status: {response.status_code}")
        print(f"Response: {response.text[:300]}...")
        
except Exception as e:
    print(f"❌ Erro: {e}")

print("\n🎯 Teste finalizado!")