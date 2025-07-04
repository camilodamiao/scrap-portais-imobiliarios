import asyncio
from playwright.async_api import async_playwright
import json
from datetime import datetime

async def detailed_zap_analysis():
    print("🔍 Análise detalhada da página Zap com Playwright...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Manter visível
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        page = await context.new_page()
        
        try:
            url = "https://www.zapimoveis.com.br/aluguel/apartamentos/sp+campinas/"
            print(f"🌐 Acessando: {url}")
            
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            # Aguardar mais tempo para carregamento dinâmico
            print("⏳ Aguardando carregamento dinâmico...")
            await page.wait_for_timeout(8000)
            
            # Tirar screenshot para análise
            await page.screenshot(path='zap_page_screenshot.png', full_page=True)
            print("📸 Screenshot salvo: zap_page_screenshot.png")
            
            title = await page.title()
            print(f"📄 Título: {title}")
            
            # Testar múltiplos seletores comuns para cards de imóveis
            selectors_to_test = [
                # Seletores genéricos
                '.listing-item',
                '.property-card', 
                '.property-item',
                '.card',
                '.result-item',
                
                # Seletores específicos do Zap
                '[data-testid="property-card"]',
                '[data-testid="listing-card"]',
                '.listing-wrapper',
                '.property-listing',
                
                # Seletores por atributos
                '[data-position]',
                '[data-property]',
                '[data-listing]',
                
                # Seletores estruturais
                'article',
                '.item',
                '.card-container',
                
                # Possíveis classes específicas
                '.zap-card',
                '.imovel-card',
                '.anuncio'
            ]
            
            results = {}
            
            for selector in selectors_to_test:
                try:
                    elements = await page.query_selector_all(selector)
                    count = len(elements)
                    results[selector] = count
                    
                    if count > 0:
                        print(f"✅ {selector}: {count} elementos encontrados")
                        
                        # Se encontrar elementos, extrair amostra
                        if count > 0:
                            first_elem = elements[0]
                            text_content = await first_elem.inner_text()
                            html_content = await first_elem.inner_html()
                            
                            results[f"{selector}_sample_text"] = text_content[:200]
                            results[f"{selector}_sample_html"] = html_content[:300]
                    else:
                        print(f"❌ {selector}: 0 elementos")
                        
                except Exception as e:
                    results[f"{selector}_error"] = str(e)
                    print(f"⚠️ {selector}: Erro - {e}")
            
            # Buscar por elementos que contêm preços (indicativo de listagens)
            print("\n💰 Buscando elementos com preços...")
            price_selectors = [
                'text=/R\$/',  # Qualquer texto contendo R$
                '[class*="price"]',
                '[class*="valor"]', 
                '[data-testid*="price"]'
            ]
            
            for selector in price_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    count = len(elements)
                    if count > 0:
                        print(f"💰 {selector}: {count} elementos com preço")
                        results[f"price_{selector}"] = count
                        
                        # Amostra do primeiro preço
                        if count > 0:
                            first_price = await elements[0].inner_text()
                            results[f"price_{selector}_sample"] = first_price
                            print(f"   Amostra: {first_price}")
                            
                except Exception as e:
                    print(f"⚠️ Erro buscando preços com {selector}: {e}")
            
            # Examinar estrutura geral da página
            print("\n🔍 Analisando estrutura da página...")
            
            # Buscar por containers principais
            main_containers = await page.query_selector_all('main, .main, #main, .content, .container, .wrapper')
            print(f"📦 Containers principais encontrados: {len(main_containers)}")
            
            # Buscar por listas/grids que podem conter listagens
            list_containers = await page.query_selector_all('ul, ol, .grid, .list, [class*="grid"], [class*="list"]')
            print(f"📋 Listas/grids encontrados: {len(list_containers)}")
            
            # Verificar se há conteúdo carregado via JavaScript
            body_text = await page.inner_text('body')
            has_listings_text = any(keyword in body_text.lower() for keyword in ['apartamento', 'quarto', 'banheiro', 'aluguel', 'm²'])
            print(f"📝 Página contém texto de listagens: {has_listings_text}")
            
            results['analysis'] = {
                'title': title,
                'url': page.url,
                'main_containers': len(main_containers),
                'list_containers': len(list_containers),
                'has_listings_text': has_listings_text,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"❌ Erro durante análise: {e}")
            results = {'error': str(e), 'timestamp': datetime.now().isoformat()}
            
        finally:
            await browser.close()
    
    # Salvar resultados detalhados
    with open('detailed_analysis_result.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print("💾 Análise salva em 'detailed_analysis_result.json'")
    print("📸 Verifique também o screenshot: zap_page_screenshot.png")
    
    return results

if __name__ == "__main__":
    asyncio.run(detailed_zap_analysis())