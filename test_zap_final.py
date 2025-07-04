import asyncio
from playwright.async_api import async_playwright
import json
import re
from datetime import datetime

async def extract_zap_listings():
    print("🎯 Extração definitiva do Zap Imóveis...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        
        page = await context.new_page()
        
        try:
            url = "https://www.zapimoveis.com.br/aluguel/apartamentos/sp+campinas/"
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            print("⏳ Aguardando carregamento...")
            await page.wait_for_timeout(8000)
            
            # Estratégia: Encontrar elementos que contêm preços no formato R$ X.XXX
            print("💰 Buscando elementos com preços estruturados...")
            
            # Regex para preços brasileiros
            price_pattern = r'R\$\s*\d{1,3}(?:\.\d{3})*(?:,\d{2})?'
            
            # Buscar todos os elementos que contêm preços
            price_elements = await page.evaluate('''
                () => {
                    const priceRegex = /R\\$\\s*\\d{1,3}(?:\\.\\d{3})*(?:,\\d{2})?/;
                    const allElements = Array.from(document.querySelectorAll('*'));
                    const elementsWithPrices = [];
                    
                    allElements.forEach((element, index) => {
                        const text = element.textContent || '';
                        if (priceRegex.test(text) && element.children.length <= 10) {
                            // Pegar elemento pai para contexto
                            let container = element;
                            let depth = 0;
                            
                            // Subir na árvore DOM até encontrar um container razoável
                            while (container.parentElement && depth < 5) {
                                container = container.parentElement;
                                const containerText = container.textContent || '';
                                
                                // Se o container tem info de imóvel (quartos, banheiros)
                                if (containerText.includes('quarto') || 
                                    containerText.includes('banheiro') || 
                                    containerText.includes('m²')) {
                                    break;
                                }
                                depth++;
                            }
                            
                            elementsWithPrices.push({
                                price: text.match(priceRegex)[0],
                                fullText: container.textContent.substring(0, 500),
                                tagName: container.tagName,
                                className: container.className,
                                index: index
                            });
                        }
                    });
                    
                    return elementsWithPrices;
                }
            ''')
            
            print(f"🏠 Encontrados {len(price_elements)} elementos com contexto de imóvel")
            
            # Filtrar e estruturar dados
            listings = []
            
            for i, elem in enumerate(price_elements):
                text = elem['fullText']
                
                # Filtrar apenas elementos que parecem ser listagens completas
                if (len(text) > 100 and 
                    ('quarto' in text.lower() or 'banheiro' in text.lower()) and
                    'jardim' not in text.lower() or 'condomin' in text.lower()):
                    
                    # Extrair informações usando regex
                    price_match = re.search(r'R\$\s*([\d.,]+)', text)
                    rooms_match = re.search(r'(\d+)\s*quarto', text.lower())
                    bath_match = re.search(r'(\d+)\s*banheiro', text.lower())
                    area_match = re.search(r'(\d+)\s*m²', text.lower())
                    
                    listing = {
                        'id': f"zap_{i+1}",
                        'price_raw': elem['price'],
                        'price_clean': price_match.group(1) if price_match else None,
                        'bedrooms': rooms_match.group(1) if rooms_match else None,
                        'bathrooms': bath_match.group(1) if bath_match else None,
                        'area_m2': area_match.group(1) if area_match else None,
                        'full_text': text,
                        'container_class': elem['className'],
                        'container_tag': elem['tagName']
                    }
                    
                    # Tentar extrair endereço/bairro
                    lines = text.split('\n')
                    for line in lines:
                        if any(keyword in line.lower() for keyword in ['jardim', 'vila', 'centro', 'bairro', 'rua', 'avenida']):
                            listing['address_candidate'] = line.strip()
                            break
                    
                    listings.append(listing)
                    
                    if i < 3:  # Mostrar primeiras 3 listagens
                        print(f"\n🏠 Listagem {i+1}:")
                        print(f"   💰 Preço: {listing['price_raw']}")
                        print(f"   🛏️ Quartos: {listing['bedrooms']}")
                        print(f"   🚿 Banheiros: {listing['bathrooms']}")
                        print(f"   📐 Área: {listing['area_m2']} m²")
                        print(f"   📍 Endereço: {listing.get('address_candidate', 'N/A')}")
            
            print(f"\n✅ Total de {len(listings)} listagens estruturadas extraídas!")
            
            result = {
                'success': True,
                'total_found': len(price_elements),
                'total_structured': len(listings),
                'listings': listings,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"❌ Erro durante extração: {e}")
            result = {'success': False, 'error': str(e)}
            
        finally:
            await browser.close()
    
    # Salvar resultados
    with open('zap_final_extraction.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print("💾 Resultados salvos em 'zap_final_extraction.json'")
    
    return result

if __name__ == "__main__":
    asyncio.run(extract_zap_listings())