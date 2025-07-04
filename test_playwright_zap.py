import asyncio
from playwright.async_api import async_playwright
import json
from datetime import datetime

async def test_zap_playwright():
    print("🎭 Testando Zap com Playwright...")
    
    async with async_playwright() as p:
        # Usar browser real para contornar Cloudflare
        browser = await p.chromium.launch(
            headless=False,  # Visível para debug
            args=['--no-sandbox', '--disable-web-security']
        )
        
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        page = await context.new_page()
        
        try:
            print("🌐 Acessando página do Zap...")
            
            # URL de busca direta
            url = "https://www.zapimoveis.com.br/aluguel/apartamentos/sp+campinas/"
            
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            # Aguardar carregamento completo
            await page.wait_for_timeout(5000)
            
            # Verificar se passou pelo Cloudflare
            title = await page.title()
            print(f"📄 Título da página: {title}")
            
            if "cloudflare" in title.lower() or "checking" in title.lower():
                print("⏳ Aguardando Cloudflare Challenge...")
                await page.wait_for_timeout(10000)  # Aguardar challenge
                
            # Verificar se chegamos na página de listagens
            current_url = page.url
            print(f"🔗 URL atual: {current_url}")
            
            # Tentar localizar cards de imóveis
            cards = await page.query_selector_all('[data-testid="property-card"], .property-card, .listing-item')
            
            print(f"🏠 Cards encontrados: {len(cards)}")
            
            if len(cards) > 0:
                print("✅ SUCESSO! Conseguimos acessar as listagens!")
                
                # Extrair dados do primeiro card
                first_card = cards[0]
                
                # Tentar extrair informações básicas
                try:
                    # Título/Descrição
                    title_elem = await first_card.query_selector('h2, .property-title, [data-testid="property-title"]')
                    title = await title_elem.inner_text() if title_elem else "N/A"
                    
                    # Preço
                    price_elem = await first_card.query_selector('.price, [data-testid="price"], .property-price')
                    price = await price_elem.inner_text() if price_elem else "N/A"
                    
                    # Endereço
                    address_elem = await first_card.query_selector('.address, [data-testid="address"], .property-address')
                    address = await address_elem.inner_text() if address_elem else "N/A"
                    
                    print(f"\n🏠 Primeira listagem:")
                    print(f"   Título: {title}")
                    print(f"   Preço: {price}")
                    print(f"   Endereço: {address}")
                    
                    result = {
                        "success": True,
                        "total_cards": len(cards),
                        "sample_listing": {
                            "title": title,
                            "price": price,
                            "address": address
                        },
                        "timestamp": datetime.now().isoformat()
                    }
                    
                except Exception as e:
                    print(f"⚠️ Erro ao extrair dados: {e}")
                    result = {
                        "success": True,
                        "total_cards": len(cards),
                        "extraction_error": str(e),
                        "timestamp": datetime.now().isoformat()
                    }
                    
            else:
                print("❌ Nenhum card de imóvel encontrado")
                result = {
                    "success": False,
                    "error": "No property cards found",
                    "page_title": title,
                    "current_url": current_url,
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            print(f"❌ Erro durante navegação: {e}")
            result = {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            
        finally:
            await browser.close()
            
    # Salvar resultado
    with open('test_playwright_result.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print("💾 Resultado salvo em 'test_playwright_result.json'")
    return result

if __name__ == "__main__":
    asyncio.run(test_zap_playwright())