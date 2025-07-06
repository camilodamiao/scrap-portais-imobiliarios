#!/usr/bin/env python3
"""
OLX Scraper v1 - São José dos Campos
Coleta dados de imóveis para aluguel com 3+ quartos e 2+ vagas
"""

import asyncio
import json
import logging
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

from playwright.async_api import async_playwright, Page, Browser

# Criar diretórios necessários antes de configurar logging
Path("logs").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)
Path("checkpoints").mkdir(exist_ok=True)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/olx_scraper_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OLXScraper:
    """Scraper para coletar dados de imóveis da OLX"""
    
    def __init__(self):
        self.base_url = "https://www.olx.com.br/imoveis/aluguel/estado-sp/vale-do-paraiba-e-litoral-norte/sao-jose-dos-campos"
        self.filters = "?sf=1&gsp=2&ros=3&ros=4&ros=5"  # 3+ quartos, 2+ vagas
        self.data_dir = Path("data")
        self.checkpoint_dir = Path("checkpoints")
        self.logs_dir = Path("logs")
        
        # Criar diretórios se não existirem
        for dir_path in [self.data_dir, self.checkpoint_dir, self.logs_dir]:
            dir_path.mkdir(exist_ok=True)
        
        self.collected_properties = []
        self.collected_ids = set()
        self.checkpoint_file = self.checkpoint_dir / "olx_checkpoint.json"
        
    def load_checkpoint(self) -> Dict:
        """Carrega checkpoint se existir"""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                self.collected_ids = set(checkpoint.get('collected_ids', []))
                logger.info(f"Checkpoint carregado: {checkpoint['total_collected']} imóveis já coletados")
                return checkpoint
        return {
            'last_page': 0,
            'total_collected': 0,
            'collected_ids': [],
            'last_update': None,
            'stats': {'by_type': {}, 'by_bedrooms': {}}
        }
    
    def save_checkpoint(self, page_num: int):
        """Salva checkpoint com estatísticas"""
        stats = self.calculate_stats()
        checkpoint = {
            'last_page': page_num,
            'total_collected': len(self.collected_properties),
            'collected_ids': list(self.collected_ids),
            'last_update': datetime.now().isoformat(),
            'stats': stats
        }
        with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
        logger.info(f"Checkpoint salvo: página {page_num}, {len(self.collected_properties)} imóveis")
    
    def extract_number(self, text: str) -> Optional[int]:
        """Extrai número de um texto"""
        if not text:
            return None
        match = re.search(r'\d+', text.replace('.', ''))
        return int(match.group()) if match else None
    
    def extract_price(self, text: str) -> Optional[float]:
        """Extrai preço de um texto"""
        if not text:
            return None
        # Remove R$, pontos e espaços, mantém vírgula
        clean = re.sub(r'[R$\s.]', '', text)
        clean = clean.replace(',', '.')
        try:
            return float(clean)
        except:
            return None
    
    def extract_property_type(self, title: str) -> Optional[str]:
        """Extrai tipo do imóvel do título"""
        if not title:
            return None
        title_lower = title.lower()
        if any(word in title_lower for word in ['apartamento', 'apto', 'ap.']):
            return 'apartamento'
        elif any(word in title_lower for word in ['casa', 'sobrado']):
            return 'casa'
        return None
    
    def extract_id_from_url(self, url: str) -> Optional[str]:
        """Extrai ID do anúncio da URL"""
        if not url:
            return None
        match = re.search(r'-(\d+)(?:\?|$)', url)
        return match.group(1) if match else None
    
    async def extract_property_data(self, card, page: Page) -> Optional[Dict]:
        """Extrai dados de um card de imóvel"""
        try:
            # Link e ID
            link_element = await card.query_selector('a.olx-adcard__link')
            if not link_element:
                return None
            
            url = await link_element.get_attribute('href')
            if not url:
                return None
            
            # Garantir URL completa
            if not url.startswith('http'):
                url = f"https://www.olx.com.br{url}"
            
            property_id = self.extract_id_from_url(url)
            if not property_id or property_id in self.collected_ids:
                return None
            
            # Título
            title_element = await card.query_selector('h2.olx-adcard__title')
            title = await title_element.text_content() if title_element else None
            
            # Preço
            price_element = await card.query_selector('h3.olx-adcard__price')
            price_text = await price_element.text_content() if price_element else None
            price = self.extract_price(price_text)
            
            # Skip se não tem preço
            if not price:
                return None
            
            # IPTU e Condomínio
            iptu = None
            condo_fee = None
            price_info_elements = await card.query_selector_all('div[data-testid="adcard-price-info"]')
            
            for element in price_info_elements:
                text = await element.text_content()
                if text:
                    if 'IPTU' in text:
                        iptu = self.extract_price(text)
                    elif 'Condomínio' in text:
                        condo_fee = self.extract_price(text)
            
            # Localização
            location_element = await card.query_selector('p.olx-adcard__location')
            location_text = await location_element.text_content() if location_element else None
            
            # Extrair bairro e cidade
            neighborhood = None
            if location_text:
                parts = location_text.split(',')
                if len(parts) >= 2:
                    neighborhood = parts[1].strip()
            
            # Detalhes (quartos, área, vagas, banheiros)
            bedrooms = None
            area = None
            parking_spaces = None
            bathrooms = None
            
            # Quartos
            bedrooms_element = await card.query_selector('div.olx-adcard__detail[aria-label*="quartos"]')
            if bedrooms_element:
                bedrooms_text = await bedrooms_element.text_content()
                bedrooms = self.extract_number(bedrooms_text)
            
            # Área
            area_element = await card.query_selector('div.olx-adcard__detail[aria-label*="metros"]')
            if area_element:
                area_text = await area_element.text_content()
                area = self.extract_number(area_text)
            
            # Vagas
            parking_element = await card.query_selector('div.olx-adcard__detail[aria-label*="vagas"]')
            if parking_element:
                parking_text = await parking_element.text_content()
                parking_spaces = self.extract_number(parking_text)
            
            # Banheiros
            bathroom_element = await card.query_selector('div.olx-adcard__detail[aria-label*="banheiro"]')
            if bathroom_element:
                bathroom_text = await bathroom_element.text_content()
                bathrooms = self.extract_number(bathroom_text)
            
            # Montar objeto do imóvel
            property_data = {
                "portal": "olx",
                "id": property_id,
                "url": url,
                "property_type": self.extract_property_type(title),
                "title": title.strip() if title else None,
                "price": price,
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "parking_spaces": parking_spaces,
                "area": area,
                "neighborhood": neighborhood,
                "city": "São José dos Campos",
                "state": "SP",
                "address": None,  # OLX geralmente não mostra endereço completo
                "condo_fee": condo_fee,
                "iptu": iptu,
                "collected_at": datetime.now().isoformat(),
                "listing_date": None  # Poderia extrair de p.olx-adcard__date se necessário
            }
            
            return property_data
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados do card: {e}")
            return None
    
    async def scrape_page(self, page: Page, page_num: int) -> List[Dict]:
        """Raspa uma página de resultados"""
        # Construir URL com paginação
        url = f"{self.base_url}{self.filters}"
        if page_num > 1:
            url += f"&o={page_num}"
        
        logger.info(f"Acessando página {page_num}: {url}")
        
        try:
            # Navegar para a página
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            # Aguardar cards carregarem
            await page.wait_for_selector('section.olx-adcard', timeout=10000)
            
            # Delay aleatório
            await asyncio.sleep(random.uniform(3, 6))
            
            # Scroll para carregar todos os cards
            for i in range(3):
                await page.evaluate('window.scrollBy(0, window.innerHeight)')
                await asyncio.sleep(random.uniform(0.5, 1.5))
            
            # Buscar todos os cards
            cards = await page.query_selector_all('section.olx-adcard')
            logger.info(f"Página {page_num}: {len(cards)} cards encontrados")
            
            # Extrair dados de cada card
            page_properties = []
            for i, card in enumerate(cards):
                property_data = await self.extract_property_data(card, page)
                if property_data:
                    page_properties.append(property_data)
                    self.collected_ids.add(property_data['id'])
                    logger.debug(f"Imóvel coletado: {property_data['title'][:50]}... - R$ {property_data['price']}")
                
                # Pequeno delay entre cards
                if i % 5 == 0:
                    await asyncio.sleep(random.uniform(0.3, 0.7))
            
            logger.info(f"Página {page_num}: {len(page_properties)} imóveis válidos extraídos")
            return page_properties
            
        except Exception as e:
            logger.error(f"Erro ao processar página {page_num}: {e}")
            return []
    
    def calculate_stats(self) -> Dict:
        """Calcula estatísticas dos imóveis coletados"""
        if not self.collected_properties:
            return {'by_type': {}, 'by_bedrooms': {}}
        
        # Estatísticas por tipo
        by_type = {}
        for prop in self.collected_properties:
            prop_type = prop.get('property_type', 'não identificado')
            by_type[prop_type] = by_type.get(prop_type, 0) + 1
        
        # Estatísticas por quartos
        by_bedrooms = {}
        for prop in self.collected_properties:
            bedrooms = str(prop.get('bedrooms', 'não informado'))
            by_bedrooms[bedrooms] = by_bedrooms.get(bedrooms, 0) + 1
        
        # Estatísticas de preço
        prices = [p['price'] for p in self.collected_properties if p.get('price')]
        price_stats = {
            'min': min(prices) if prices else 0,
            'max': max(prices) if prices else 0,
            'avg': sum(prices) / len(prices) if prices else 0
        }
        
        return {
            'by_type': by_type,
            'by_bedrooms': by_bedrooms,
            'price_stats': price_stats
        }
    
    def save_data(self):
        """Salva dados coletados em JSON"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.data_dir / f"olx_data_v1_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.collected_properties, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Dados salvos em: {filename}")
        logger.info(f"Total de imóveis: {len(self.collected_properties)}")
    
    def print_statistics(self):
        """Imprime estatísticas finais"""
        stats = self.calculate_stats()
        
        print("\n" + "="*60)
        print("ESTATÍSTICAS FINAIS")
        print("="*60)
        print(f"Total de imóveis coletados: {len(self.collected_properties)}")
        
        print("\nPor tipo de imóvel:")
        for prop_type, count in stats['by_type'].items():
            print(f"  {prop_type}: {count}")
        
        print("\nPor número de quartos:")
        for bedrooms, count in sorted(stats['by_bedrooms'].items()):
            print(f"  {bedrooms} quartos: {count}")
        
        if stats.get('price_stats'):
            print("\nEstatísticas de preço:")
            print(f"  Mínimo: R$ {stats['price_stats']['min']:,.2f}")
            print(f"  Máximo: R$ {stats['price_stats']['max']:,.2f}")
            print(f"  Médio: R$ {stats['price_stats']['avg']:,.2f}")
        
        print("="*60 + "\n")
    
    async def run(self, target_pages: int = 5):
        """Executa o scraper"""
        logger.info("Iniciando OLX Scraper v1")
        logger.info(f"Meta: coletar ~{target_pages * 40} imóveis ({target_pages} páginas)")
        
        # Carregar checkpoint
        checkpoint = self.load_checkpoint()
        start_page = checkpoint['last_page'] + 1 if checkpoint['last_page'] > 0 else 1
        
        # Se já temos dados do checkpoint, carregar
        if checkpoint['total_collected'] > 0:
            # Por enquanto, vamos apenas continuar com os IDs já coletados
            # Em uma versão futura, poderíamos recarregar os dados completos
            logger.info(f"Continuando da página {start_page}")
        
        async with async_playwright() as p:
            # Iniciar navegador (headed para validação)
            browser = await p.chromium.launch(
                headless=False,
                args=['--disable-blink-features=AutomationControlled']
            )
            
            # Criar contexto com user agent realista
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1366, 'height': 768}
            )
            
            page = await context.new_page()
            
            try:
                # Coletar páginas
                for page_num in range(start_page, start_page + target_pages):
                    logger.info(f"\n--- Processando página {page_num}/{start_page + target_pages - 1} ---")
                    
                    # Raspar página
                    page_properties = await self.scrape_page(page, page_num)
                    
                    # Adicionar aos dados coletados
                    self.collected_properties.extend(page_properties)
                    
                    # Salvar checkpoint a cada página
                    self.save_checkpoint(page_num)
                    
                    # Salvar dados incrementalmente a cada 100 imóveis
                    if len(self.collected_properties) % 100 == 0 and len(self.collected_properties) > 0:
                        self.save_data()
                    
                    # Delay entre páginas
                    if page_num < start_page + target_pages - 1:
                        delay = random.uniform(5, 8)
                        logger.info(f"Aguardando {delay:.1f}s antes da próxima página...")
                        await asyncio.sleep(delay)
                
                # Salvar dados finais
                self.save_data()
                
                # Imprimir estatísticas
                self.print_statistics()
                
            except Exception as e:
                logger.error(f"Erro durante execução: {e}")
                # Salvar o que foi coletado até agora
                if self.collected_properties:
                    self.save_data()
            
            finally:
                await context.close()
                await browser.close()
        
        logger.info("Scraper finalizado!")

if __name__ == "__main__":
    scraper = OLXScraper()
    asyncio.run(scraper.run(target_pages=5))