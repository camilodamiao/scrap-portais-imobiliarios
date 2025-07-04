import asyncio
import json
import re
from datetime import datetime
from playwright.async_api import async_playwright
from typing import Dict, List, Optional
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ZapScraperRefined:
    """Scraper refinado para Zap Imóveis usando Playwright"""
    
    def __init__(self):
        self.base_url = "https://www.zapimoveis.com.br"
        self.data = []
        
    async def extract_listing_data(self, element) -> Optional[Dict]:
        """Extrai dados de uma listagem individual"""
        try:
            # Obter texto completo do elemento
            text = await element.inner_text()
            
            # Patterns de regex melhorados
            patterns = {
                'price': r'R\$\s*([\d\.]+)(?:/mês)?',
                'bedrooms': r'(\d+)\s*(?:quartos?|Quartos?)',
                'bathrooms': r'(\d+)\s*(?:banheiros?|Banheiros?)',
                'parking': r'(\d+)\s*(?:vagas?|Vagas?)',
                'area': r'(\d+)\s*m²',
                'condo_fee': r'Condomínio\s*R\$\s*([\d\.]+)',
                'neighborhood': r'([^,\n]+),\s*([^,\n]+),\s*([A-Z]{2})',  # Bairro, Cidade, Estado
            }
            
            # Extrair dados usando patterns
            data = {
                'portal': 'zap_imoveis',
                'collected_at': datetime.now().isoformat(),
                'raw_text': text[:500]  # Primeiros 500 chars para debug
            }
            
            # Preço
            price_match = re.search(patterns['price'], text)
            if price_match:
                data['price'] = float(price_match.group(1).replace('.', ''))
                data['price_type'] = 'RENTAL' if '/mês' in text else 'SALE'
            
            # Características do imóvel
            for key, pattern in patterns.items():
                if key not in ['price', 'neighborhood']:
                    match = re.search(pattern, text)
                    if match:
                        data[key] = int(match.group(1)) if key != 'condo_fee' else float(match.group(1).replace('.', ''))
            
            # Endereço
            addr_match = re.search(patterns['neighborhood'], text)
            if addr_match:
                data['neighborhood'] = addr_match.group(1).strip()
                data['city'] = addr_match.group(2).strip()
                data['state'] = addr_match.group(3).strip()
            
            # Tentar extrair o link do anúncio
            try:
                link_element = await element.query_selector('a[href*="/imoveis/"]')
                if link_element:
                    href = await link_element.get_attribute('href')
                    data['url'] = f"{self.base_url}{href}" if href.startswith('/') else href
                    # Extrair ID do URL
                    id_match = re.search(r'/(\d+)/?$', href)
                    if id_match:
                        data['id'] = id_match.group(1)
            except:
                pass
            
            # Validar dados mínimos
            if 'price' in data and any(k in data for k in ['bedrooms', 'area']):
                return data
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados: {str(e)}")
        
        return None
    
    async def scrape_page(self, page, url: str) -> List[Dict]:
        """Scraping de uma página específica"""
        logger.info(f"Acessando: {url}")
        
        await page.goto(url, wait_until='networkidle')
        
        # Aguardar carregamento dinâmico
        await page.wait_for_timeout(8000)
        
        # Scroll para carregar mais conteúdo
        for i in range(3):
            await page.evaluate('window.scrollBy(0, window.innerHeight)')
            await page.wait_for_timeout(2000)
        
        # Estratégias múltiplas para encontrar listagens
        listings_data = []
        
        # Estratégia 1: Buscar por elementos com preço
        price_elements = await page.query_selector_all('*:has-text("R$")')
        logger.info(f"Encontrados {len(price_elements)} elementos com preço")
        
        for element in price_elements:
            # Subir na árvore DOM para encontrar o container completo
            parent = element
            for _ in range(5):  # Subir até 5 níveis
                try:
                    parent = await parent.query_selector('xpath=..')
                    if parent:
                        text = await parent.inner_text()
                        # Verificar se tem informações suficientes
                        if all(keyword in text for keyword in ['R$', 'm²']):
                            data = await self.extract_listing_data(parent)
                            if data and data not in listings_data:
                                listings_data.append(data)
                                break
                except:
                    break
        
        # Estratégia 2: Buscar por cards/containers comuns
        selectors = [
            '[data-testid*="card"]',
            '[class*="result-card"]',
            '[class*="listing"]',
            'article',
            '[role="article"]'
        ]
        
        for selector in selectors:
            elements = await page.query_selector_all(selector)
            for element in elements:
                text = await element.inner_text()
                if 'R$' in text and 'm²' in text:
                    data = await self.extract_listing_data(element)
                    if data and data not in listings_data:
                        listings_data.append(data)
        
        # Remover duplicatas baseadas no ID ou texto similar
        unique_listings = []
        seen_ids = set()
        seen_texts = set()
        
        for listing in listings_data:
            listing_id = listing.get('id', '')
            listing_text = listing.get('raw_text', '')[:100]
            
            if listing_id and listing_id not in seen_ids:
                seen_ids.add(listing_id)
                unique_listings.append(listing)
            elif listing_text and listing_text not in seen_texts:
                seen_texts.add(listing_text)
                unique_listings.append(listing)
        
        logger.info(f"Extraídas {len(unique_listings)} listagens únicas")
        return unique_listings
    
    async def scrape_multiple_pages(self, max_pages: int = 3):
        """Scraping com paginação"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,  # False para passar pelo Cloudflare
                args=['--disable-blink-features=AutomationControlled']
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            
            page = await context.new_page()
            
            # Configurar para Sao Jose dos Campos, aluguel
            base_search_url = f"{self.base_url}/aluguel/imoveis/sp+sao-jose-dos-campos/"
            
            for page_num in range(1, max_pages + 1):
                url = base_search_url if page_num == 1 else f"{base_search_url}?pagina={page_num}"
                
                try:
                    listings = await self.scrape_page(page, url)
                    self.data.extend(listings)
                    
                    # Rate limiting
                    if page_num < max_pages:
                        await asyncio.sleep(5)
                        
                except Exception as e:
                    logger.error(f"Erro na página {page_num}: {str(e)}")
                    
            await browser.close()
    
    def save_data(self, filename: str = "zap_listings_refined.json"):
        """Salvar dados em JSON"""
        output = {
            'metadata': {
                'total_listings': len(self.data),
                'scraped_at': datetime.now().isoformat(),
                'portal': 'zap_imoveis',
                'location': 'Sao Jose dos Campos, SP'
            },
            'listings': self.data
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Dados salvos em {filename}")
        
        # Estatísticas
        if self.data:
            prices = [l['price'] for l in self.data if 'price' in l]
            if prices:
                logger.info(f"Preço médio: R$ {sum(prices)/len(prices):,.2f}")
                logger.info(f"Preço mínimo: R$ {min(prices):,.2f}")
                logger.info(f"Preço máximo: R$ {max(prices):,.2f}")

async def main():
    """Função principal"""
    scraper = ZapScraperRefined()
    
    # Scraping de múltiplas páginas
    logger.info("Iniciando scraping do Zap Imóveis...")
    await scraper.scrape_multiple_pages(max_pages=2)  # Começar com 2 páginas
    
    # Salvar resultados
    scraper.save_data()
    
    # Mostrar amostra dos dados
    if scraper.data:
        logger.info("\n=== AMOSTRA DOS DADOS ===")
        for i, listing in enumerate(scraper.data[:3]):
            logger.info(f"\nListagem {i+1}:")
            for key, value in listing.items():
                if key != 'raw_text':
                    logger.info(f"  {key}: {value}")

if __name__ == "__main__":
    asyncio.run(main())