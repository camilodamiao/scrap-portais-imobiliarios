import asyncio
import json
import re
import os
from datetime import datetime
from playwright.async_api import async_playwright
from typing import Dict, List, Optional, Set
import logging
import random
import hashlib
from pathlib import Path

# Configurar logging detalhado
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler('zap_scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ZapScraperProduction:
    """Scraper de produção para Zap Imóveis com salvamento incremental"""
    
    def __init__(self, checkpoint_dir: str = "checkpoints"):
        self.base_url = "https://www.zapimoveis.com.br"
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)
        self.data = []
        self.processed_ids = set()
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def load_checkpoint(self) -> Dict:
        """Carrega o último checkpoint se existir"""
        checkpoint_file = self.checkpoint_dir / "latest_checkpoint.json"
        
        if checkpoint_file.exists():
            try:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)
                    logger.info(f"Checkpoint carregado: {checkpoint['total_collected']} imóveis já coletados")
                    logger.info(f"Última página processada: {checkpoint.get('last_page', 0)}")
                    return checkpoint
            except Exception as e:
                logger.error(f"Erro ao carregar checkpoint: {e}")
        
        return {
            'last_page': 0,
            'total_collected': 0,
            'processed_ids': [],
            'session_id': self.session_id
        }
    
    def save_checkpoint(self, page_num: int):
        """Salva o progresso atual"""
        checkpoint = {
            'last_page': page_num,
            'total_collected': len(self.data),
            'processed_ids': list(self.processed_ids),
            'session_id': self.session_id,
            'last_update': datetime.now().isoformat()
        }
        
        checkpoint_file = self.checkpoint_dir / "latest_checkpoint.json"
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Checkpoint salvo: Página {page_num}, Total: {len(self.data)} imóveis")
    
    def save_incremental_data(self):
        """Salva dados incrementalmente"""
        # Arquivo principal com todos os dados
        output_file = f"zap_data_{self.session_id}.json"
        
        output = {
            'metadata': {
                'portal': 'zap_imoveis',
                'session_id': self.session_id,
                'total_listings': len(self.data),
                'last_update': datetime.now().isoformat(),
                'filters': {
                    'location': 'São José dos Campos - SP',
                    'neighborhoods': ['Vila Adyana', 'Vila Ema', 'Jardim Aquarius', 
                                    'Jardim Esplanada', 'Jardim das Colinas', 
                                    'Jardim Apolo', 'Urbanova', 'Jardim das Indústrias'],
                    'min_bedrooms': 3,
                    'min_parking': 2,
                    'type': 'RENTAL'
                }
            },
            'listings': self.data
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Dados salvos em {output_file}")
        
        # Backup incremental
        backup_file = self.checkpoint_dir / f"backup_{self.session_id}_{len(self.data)}.json"
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False)
    
    def create_listing_hash(self, listing: Dict) -> str:
        """Cria hash único para o imóvel baseado em características"""
        # Usa endereço + área + quartos para criar identificador único
        hash_string = f"{listing.get('address', '')}{listing.get('area', 0)}{listing.get('bedrooms', 0)}"
        return hashlib.md5(hash_string.encode()).hexdigest()
    
    async def extract_listing_data(self, element) -> Optional[Dict]:
        """Extrai todos os dados relevantes de uma listagem"""
        try:
            text = await element.inner_text()
            
            # Patterns melhorados para capturar mais dados
            patterns = {
                'price': r'R\$\s*([\d\.]+)(?:/mês)?',
                'bedrooms': r'(\d+)\s*(?:quartos?|Quartos?|quarto|Quarto)',
                'bathrooms': r'(\d+)\s*(?:banheiros?|Banheiros?|banheiro|Banheiro)',
                'parking': r'(\d+)\s*(?:vagas?|Vagas?|vaga|Vaga)',
                'area': r'(\d+)\s*m²',
                'condo_fee': r'Condomínio\s*R\$\s*([\d\.]+)',
                'property_type': r'(Casa|Apartamento|Sobrado|Kitnet|Studio|Cobertura|Flat)',
                'address_pattern': r'([^,\n]+),\s*([^,\n]+)(?:,\s*([^,\n]+))?'
            }
            
            # Dados básicos
            data = {
                'portal': 'zap_imoveis',
                'collected_at': datetime.now().isoformat(),
                'raw_text': text[:1000]  # Guardar mais texto para debug
            }
            
            # Extrair preço
            price_match = re.search(patterns['price'], text)
            if price_match:
                data['price'] = float(price_match.group(1).replace('.', ''))
                data['price_type'] = 'RENTAL'  # Já filtrado para aluguel
            
            # Extrair características
            for key, pattern in patterns.items():
                if key not in ['price', 'address_pattern']:
                    match = re.search(pattern, text)
                    if match:
                        if key == 'condo_fee':
                            data[key] = float(match.group(1).replace('.', ''))
                        elif key == 'property_type':
                            data[key] = match.group(1)
                        else:
                            data[key] = int(match.group(1))
            
            # Extrair endereço completo
            addr_match = re.search(patterns['address_pattern'], text)
            if addr_match:
                data['street'] = addr_match.group(1).strip() if addr_match.group(1) else ''
                data['neighborhood'] = addr_match.group(2).strip() if addr_match.group(2) else ''
                data['city'] = addr_match.group(3).strip() if addr_match.group(3) else 'São José dos Campos'
                data['address'] = f"{data['street']}, {data['neighborhood']}, {data['city']}"
            
            # Tentar extrair link e ID
            try:
                link_element = await element.query_selector('a[href*="/imoveis/"]')
                if link_element:
                    href = await link_element.get_attribute('href')
                    data['url'] = f"{self.base_url}{href}" if href.startswith('/') else href
                    
                    # Extrair ID do URL
                    id_match = re.search(r'/(\d+)/?(?:\?|$)', href)
                    if id_match:
                        data['id'] = id_match.group(1)
                    else:
                        # Criar ID baseado no hash se não encontrar no URL
                        data['id'] = self.create_listing_hash(data)
            except:
                data['id'] = self.create_listing_hash(data)
            
            # Validar dados mínimos necessários
            if 'price' in data and 'id' in data:
                return data
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados: {str(e)}")
        
        return None
    
    async def scrape_page(self, page, page_num: int) -> List[Dict]:
        """Scraping de uma página específica"""
        # URL com filtros aplicados
        if page_num == 1:
            url = ("https://www.zapimoveis.com.br/aluguel/imoveis/sp+sao-jose-dos-campos/"
                   "3-quartos/?onde=%2CS%C3%A3o+Paulo%2CS%C3%A3o+Jos%C3%A9+dos+Campos%2C%2C%2C%2C%2Ccity"
                   "%2CBR%3ESao+Paulo%3ENULL%3ESao+Jose+dos+Campos%2C-23.21984%2C-45.891566%2C%3B"
                   "%2CS%C3%A3o+Paulo%2CS%C3%A3o+Jos%C3%A9+dos+Campos%2C%2CJardim+Aquarius%2C%2C%2Cneighborhood"
                   "%2CBR%3ESao+Paulo%3ENULL%3ESao+Jose+dos+Campos%3EBarrios%3EJardim+Aquarius%2C-23.218452"
                   "%2C-45.902781%2C%3B%2CS%C3%A3o+Paulo%2CS%C3%A3o+Jos%C3%A9+dos+Campos%2C%2CParque+Residencial"
                   "+Aquarius%2C%2C%2Cneighborhood%2CBR%3ESao+Paulo%3ENULL%3ESao+Jose+dos+Campos%3EBarrios"
                   "%3EParque+Res+Aquarius%2C-23.218452%2C-45.902781%2C%3B%2CS%C3%A3o+Paulo%2CS%C3%A3o+Jos%C3%A9"
                   "+dos+Campos%2C%2CVila+Ema%2C%2C%2Cneighborhood%2CBR%3ESao+Paulo%3ENULL%3ESao+Jose+dos+Campos"
                   "%3EBarrios%3EVila+Ema%2C-23.205804%2C-45.900404%2C%3B%2CS%C3%A3o+Paulo%2CS%C3%A3o+Jos%C3%A9"
                   "+dos+Campos%2C%2CVila+Adyana%2C%2C%2Cneighborhood%2CBR%3ESao+Paulo%3ENULL%3ESao+Jose+dos"
                   "+Campos%3EBarrios%3EVila+Adyana%2C-23.196596%2C-45.892681%2C%3B%2CS%C3%A3o+Paulo%2CS%C3%A3o"
                   "+Jos%C3%A9+dos+Campos%2C%2CUrbanova%2C%2C%2Cneighborhood%2CBR%3ESao+Paulo%3ENULL%3ESao+Jose"
                   "+dos+Campos%3EBarrios%3EUrbanova%2C-23.20301%2C-45.959855%2C&quartos=3%2C4&vagas=2&transacao=aluguel")
        else:
            url = f"{url}&pagina={page_num}"
        
        logger.info(f"Acessando página {page_num}: {url[:100]}...")
        
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            
            # Aguardar carregamento
            await asyncio.sleep(random.uniform(3, 5))
            
            # Scroll para carregar conteúdo dinâmico
            for i in range(3):
                await page.evaluate('window.scrollBy(0, window.innerHeight)')
                await asyncio.sleep(random.uniform(1, 2))
            
            # Buscar listagens
            listings_data = []
            
            # Estratégia 1: Buscar elementos com preço
            price_elements = await page.query_selector_all('*:has-text("R$")')
            logger.info(f"Página {page_num}: {len(price_elements)} elementos com preço encontrados")
            
            for element in price_elements:
                try:
                    # Subir na árvore DOM para pegar container completo
                    parent = element
                    for _ in range(5):
                        parent = await parent.query_selector('xpath=..')
                        if parent:
                            text = await parent.inner_text()
                            if all(keyword in text for keyword in ['R$', 'm²']):
                                data = await self.extract_listing_data(parent)
                                if data and data['id'] not in self.processed_ids:
                                    listings_data.append(data)
                                    self.processed_ids.add(data['id'])
                                    break
                except:
                    continue
            
            logger.info(f"Página {page_num}: {len(listings_data)} novas listagens extraídas")
            return listings_data
            
        except Exception as e:
            logger.error(f"Erro na página {page_num}: {str(e)}")
            return []
    
    async def run_scraper(self):
        """Execução principal do scraper"""
        logger.info("=== INICIANDO SCRAPER ZAP PRODUÇÃO ===")
        logger.info(f"Sessão ID: {self.session_id}")
        
        # Carregar checkpoint
        checkpoint = self.load_checkpoint()
        start_page = checkpoint['last_page'] + 1
        
        # Restaurar dados anteriores se houver
        if checkpoint['processed_ids']:
            self.processed_ids = set(checkpoint['processed_ids'])
            # Carregar dados anteriores do arquivo
            prev_file = f"zap_data_{checkpoint['session_id']}.json"
            if os.path.exists(prev_file):
                with open(prev_file, 'r', encoding='utf-8') as f:
                    prev_data = json.load(f)
                    self.data = prev_data['listings']
                    logger.info(f"Dados anteriores carregados: {len(self.data)} imóveis")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-features=IsolateOrigins,site-per-process'
                ]
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                locale='pt-BR',
                timezone_id='America/Sao_Paulo'
            )
            
            # Scripts anti-detecção
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)
            
            page = await context.new_page()
            
            # Loop principal - tentar coletar o máximo possível
            page_num = start_page
            consecutive_empty_pages = 0
            max_empty_pages = 3
            
            while consecutive_empty_pages < max_empty_pages:
                try:
                    listings = await self.scrape_page(page, page_num)
                    
                    if listings:
                        self.data.extend(listings)
                        consecutive_empty_pages = 0
                        
                        # Salvar a cada 100 novos imóveis
                        if len(self.data) % 100 == 0:
                            self.save_incremental_data()
                            self.save_checkpoint(page_num)
                            logger.info(f"Salvamento incremental: {len(self.data)} imóveis total")
                    else:
                        consecutive_empty_pages += 1
                        logger.warning(f"Página {page_num} sem novos dados. Páginas vazias consecutivas: {consecutive_empty_pages}")
                    
                    # Salvar checkpoint após cada página
                    self.save_checkpoint(page_num)
                    
                    # Delay entre páginas
                    await asyncio.sleep(random.uniform(3, 6))
                    
                    page_num += 1
                    
                except Exception as e:
                    logger.error(f"Erro crítico na página {page_num}: {str(e)}")
                    self.save_incremental_data()
                    self.save_checkpoint(page_num - 1)
                    break
            
            await browser.close()
            
        # Salvar dados finais
        self.save_incremental_data()
        self.cleanup_checkpoint()
        
        # Estatísticas finais
        self.print_statistics()
    
    def cleanup_checkpoint(self):
        """Limpa checkpoint após conclusão bem-sucedida"""
        checkpoint_file = self.checkpoint_dir / "latest_checkpoint.json"
        if checkpoint_file.exists():
            # Renomear para histórico
            history_file = self.checkpoint_dir / f"completed_{self.session_id}.json"
            checkpoint_file.rename(history_file)
            logger.info("Checkpoint movido para histórico")
    
    def print_statistics(self):
        """Imprime estatísticas da coleta"""
        logger.info("\n=== ESTATÍSTICAS FINAIS ===")
        logger.info(f"Total de imóveis coletados: {len(self.data)}")
        
        if self.data:
            prices = [l['price'] for l in self.data if 'price' in l]
            areas = [l['area'] for l in self.data if 'area' in l]
            
            if prices:
                logger.info(f"Preço médio: R$ {sum(prices)/len(prices):,.2f}")
                logger.info(f"Preço mínimo: R$ {min(prices):,.2f}")
                logger.info(f"Preço máximo: R$ {max(prices):,.2f}")
            
            if areas:
                logger.info(f"Área média: {sum(areas)/len(areas):.1f} m²")
            
            # Distribuição por bairro
            neighborhoods = {}
            for listing in self.data:
                neighborhood = listing.get('neighborhood', 'N/A')
                neighborhoods[neighborhood] = neighborhoods.get(neighborhood, 0) + 1
            
            logger.info("\nDistribuição por bairro:")
            for neighborhood, count in sorted(neighborhoods.items(), key=lambda x: x[1], reverse=True)[:10]:
                logger.info(f"  {neighborhood}: {count} imóveis")

async def main():
    """Função principal"""
    scraper = ZapScraperProduction()
    await scraper.run_scraper()

if __name__ == "__main__":
    asyncio.run(main())