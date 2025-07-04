import zapimoveis_scraper as zap
from pprint import pprint
import json
from datetime import datetime

print("🔍 Testando GeovRodri zapimoveis-scraper...")
print(f"⏰ Timestamp: {datetime.now()}")

# Teste básico - apenas 1 página para validar
print("\n📍 Testando busca em Campinas...")
try:
    results = zap.search(
        localization="sp+campinas",  # Campinas, SP
        num_pages=1,                 # Apenas 1 página para teste
        acao="aluguel",              # Aluguel
        tipo="apartamentos",         # Apartamentos
        time_to_wait=2               # 2 segundos entre requests
    )
    
    print(f"✅ Sucesso! Encontrados {len(results)} resultados")
    
    if results:
        print("\n🏠 Primeira listagem encontrada:")
        first = results[0]
        
        # Mostrar todos os atributos disponíveis
        print(f"📝 Descrição: {first.description[:100]}..." if hasattr(first, 'description') else "Descrição: N/A")
        print(f"💰 Preço: {first.price}" if hasattr(first, 'price') else "Preço: N/A")
        print(f"🏢 Taxa cond.: {first.condo_fee}" if hasattr(first, 'condo_fee') else "Taxa cond.: N/A")
        print(f"🛏️ Quartos: {first.bedrooms}" if hasattr(first, 'bedrooms') else "Quartos: N/A")
        print(f"🚿 Banheiros: {first.bathrooms}" if hasattr(first, 'bathrooms') else "Banheiros: N/A")
        print(f"📐 Área: {first.total_area_m2}" if hasattr(first, 'total_area_m2') else "Área: N/A")
        print(f"🚗 Vagas: {first.vacancies}" if hasattr(first, 'vacancies') else "Vagas: N/A")
        print(f"📍 Endereço: {first.address}" if hasattr(first, 'address') else "Endereço: N/A")
        print(f"🔗 Link: {first.link}" if hasattr(first, 'link') else "Link: N/A")
        
        # Salvar resultados para análise
        data_to_save = []
        for i, item in enumerate(results):
            item_data = {}
            # Extrair todos os atributos
            for attr in ['description', 'price', 'condo_fee', 'bedrooms', 'bathrooms', 
                        'total_area_m2', 'vacancies', 'address', 'link']:
                item_data[attr] = getattr(item, attr, None)
            
            item_data['index'] = i
            item_data['collected_at'] = datetime.now().isoformat()
            data_to_save.append(item_data)
        
        # Salvar em JSON
        with open('test_results_geov.json', 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Dados salvos em 'test_results_geov.json'")
        print(f"📊 Total de {len(results)} imóveis coletados")
        
    else:
        print("⚠️ Nenhum resultado encontrado")
        
except Exception as e:
    print(f"❌ Erro durante o teste: {e}")
    print(f"Tipo do erro: {type(e)}")

print("\n🎯 Teste concluído!")