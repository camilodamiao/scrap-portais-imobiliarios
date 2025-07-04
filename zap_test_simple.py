import asyncio
from playwright.async_api import async_playwright
import json
from datetime import datetime

async def test_zap_selectors():
    """Teste simples para validar os novos seletores do Zap"""
    
    print("=== TESTE DE VALIDAÇÃO ZAP IMÓVEIS ===")
    print(f"Horário: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    async with async_playwright() as p:
        # Navegador visível para debug
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        # URL com filtros (primeira página)
        url = ("https://www.zapimoveis.com.br/aluguel/imoveis/sp+sao-jose-dos-campos/"
               "3-quartos/?onde=%2CS%C3%A3o+Paulo%2CS%C3%A3o+Jos%C3%A9+dos+Campos%2C%2C%2C%2C%2Ccity"
               "%2CBR%3ESao+Paulo%3ENULL%3ESao+Jose+dos+Campos%2C-23.21984%2C-45.891566%2C&"
               "quartos=3%2C4&vagas=2&transacao=aluguel")
        
        print(f"Acessando: {url[:80]}...\n")
        await page.goto(url)
        
        # Aguardar carregamento
        print("Aguardando carregamento da página...")
        await page.wait_for_timeout(5000)
        
        # Testar seletores
        print("\n=== TESTANDO SELETORES ===\n")
        
        # 1. Contar cards
        cards = await page.query_selector_all('li[data-cy="rp-property-cd"]')
        print(f"✓ Cards encontrados: {len(cards)}")
        
        if len(cards) == 0:
            print("❌ ERRO: Nenhum card encontrado! Verifique se a página carregou.")
            await browser.close()
            return
        
        # 2. Extrair dados dos primeiros 3 cards
        print(f"\nExtraindo dados dos primeiros 3 cards:\n")
        
        listings = []
        
        for i, card in enumerate(cards[:3]):
            print(f"--- Card {i+1} ---")
            
            try:
                # Link
                link_element = await card.query_selector('a')
                link = await link_element.get_attribute('href') if link_element else None
                print(f"Link: {link[:80] if link else 'NÃO ENCONTRADO'}...")
                
                # ID do imóvel (extrair do link)
                import re
                id_match = re.search(r'id-(\d+)', link) if link else None
                property_id = id_match.group(1) if id_match else None
                print(f"ID: {property_id if property_id else 'NÃO ENCONTRADO'}")
                
                # Localização
                location_element = await card.query_selector('[data-cy="rp-cardProperty-location-txt"]')
                location = await location_element.inner_text() if location_element else None
                print(f"Localização: {location if location else 'NÃO ENCONTRADO'}")
                
                # Rua
                street_element = await card.query_selector('[data-cy="rp-cardProperty-street-txt"]')
                street = await street_element.inner_text() if street_element else None
                print(f"Rua: {street if street else 'NÃO ENCONTRADO'}")
                
                # Área
                area_element = await card.query_selector('[data-cy="rp-cardProperty-propertyArea-txt"]')
                area = await area_element.inner_text() if area_element else None
                print(f"Área: {area if area else 'NÃO ENCONTRADO'}")
                
                # Quartos
                bedrooms_element = await card.query_selector('[data-cy="rp-cardProperty-bedroomQuantity-txt"]')
                bedrooms = await bedrooms_element.inner_text() if bedrooms_element else None
                print(f"Quartos: {bedrooms if bedrooms else 'NÃO ENCONTRADO'}")
                
                # Vagas
                parking_element = await card.query_selector('[data-cy="rp-cardProperty-parkingSpacesQuantity-txt"]')
                parking = await parking_element.inner_text() if parking_element else None
                print(f"Vagas: {parking if parking else 'NÃO ENCONTRADO'}")
                
                # Preço
                price_element = await card.query_selector('[data-cy="rp-cardProperty-price-txt"]')
                price_text = await price_element.inner_text() if price_element else None
                print(f"Preço completo: {price_text if price_text else 'NÃO ENCONTRADO'}")
                
                print()
                
                # Estruturar dados
                listing = {
                    'id': property_id,
                    'link': link,
                    'location': location,
                    'street': street,
                    'area': area,
                    'bedrooms': bedrooms,
                    'parking': parking,
                    'price_full': price_text,
                    'extracted_at': datetime.now().isoformat()
                }
                listings.append(listing)
                
            except Exception as e:
                print(f"❌ Erro no card {i+1}: {str(e)}\n")
        
        # Salvar amostra
        output = {
            'test_date': datetime.now().isoformat(),
            'total_cards_found': len(cards),
            'sample_listings': listings
        }
        
        with open('zap_test_validation.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ Teste concluído! Dados salvos em 'zap_test_validation.json'")
        print(f"✓ Total de cards na página: {len(cards)}")
        
        await browser.close()

# Executar teste
if __name__ == "__main__":
    asyncio.run(test_zap_selectors())