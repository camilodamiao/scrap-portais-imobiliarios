#!/usr/bin/env python3
"""
OLX Scraper v2 - São José dos Campos
Versão completa com retry, estatísticas avançadas e exportação CSV
"""

import asyncio
import csv
import json
import logging
import random
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PlaywrightTimeout

# Criar diretórios necessários antes de configurar logging
Path("logs").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)
Path("checkpoints").mkdir(exist_ok=True)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/olx_scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OLXScraper:
    """Scraper completo para coletar dados de imóveis da OLX"""
    
    def __init__(self):
        self.base_url = "https://www.olx.com.br/imoveis/aluguel/estado-sp/vale-do-paraiba-e-litoral-norte/sao-jose-dos-campos"
        self.filters = "?sf=1&gsp=2&ros=3&ros=4&ros=5"  # 3+ quartos, 2+ vagas
        self.data_dir = Path("data")
        self.checkpoint_dir = Path("checkpoints")
        self.logs_dir = Path("logs")
        
        self.collected_properties = []
        self.collected_ids = set()
        self.checkpoint_file = self.checkpoint_dir / "olx_checkpoint.json"
        
        # Estatísticas de execução
        self.stats = {
            'start_time': datetime.now(),
            'pages_processed': 0,
            'pages_failed': 0,
            'retries': 0,
            'empty_pages': 0,
            'errors': []
        }
        
        # Configurações
        self.max_retries = 3
        self.retry_delay = 10
        self.save_frequency = 50  # Salvar a cada X imóveis
        
    def load_checkpoint(self) -> Dict:
        """Carrega checkpoint se existir"""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
                self.collected_ids = set(checkpoint.get('collected_ids', []))
                # Recarregar propriedades se disponível
                if 'properties' in checkpoint:
                    self.collected_properties = checkpoint['properties']
                logger.info(f"Checkpoint carregado: {len(self.collected_ids)} IDs únicos")
                logger.info(f"Total de imóveis no checkpoint: {len(self.collected_properties)}")
                return checkpoint
        return {
            'last_page': 0,
            'total_collected': 0,
            'collected_ids': [],
            'properties': [],
            'last_update': None,
            'stats': {}
        }
    
    def save_checkpoint(self, page_num: int):
        """Salva checkpoint completo com dados"""
        # Converter datetime para string em execution_stats
        exec_stats = self.stats.copy()
        exec_stats['start_time'] = exec_stats['start_time'].isoformat()
        
        checkpoint = {
            'last_page': page_num,
            'total_collected': len(self.collected_properties),
            'collected_ids': list(self.collected_ids),
            'properties': self.collected_properties,  # Salvar dados completos
            'last_update': datetime.now().isoformat(),
            'stats': self.calculate_stats(),
            'execution_stats': exec_stats
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
    
    def calculate_price_per_sqm(self, price: Optional[float], area: Optional[int]) -> Optional[float]:
        """Calcula preço por metro quadrado"""
        if price and area and area > 0:
            return round(price / area, 2)
        return None
    
    async def extract_property_data(self, card, page: Page) -> Optional[Dict]:
        """Extrai dados de um card de imóvel"""
        try:
            # Link e ID
            link_element = await card.query_selector('a.olx-adcard__link')
            if not link_element:
                logger.debug("Card sem link encontrado")
                return None
            
            url = await link_element.get_attribute('href')
            if not url:
                logger.debug("Card sem URL")
                return None
            
            # Garantir URL completa
            if not url.startswith('http'):
                url = f"https://www.olx.com.br{url}"
            
            property_id = self.extract_id_from_url(url)
            if not property_id:
                logger.debug(f"Não foi possível extrair ID da URL: {url}")
                return None
                
            if property_id in self.collected_ids:
                logger.debug(f"Imóvel {property_id} já coletado anteriormente")
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
            
            # Calcular preço por m²
            price_per_sqm = self.calculate_price_per_sqm(price, area)
            
            # Montar objeto do imóvel
            property_data = {
                "portal": "olx",
                "id": property_id,
                "url": url,
                "property_type": self.extract_property_type(title),
                "title": title.strip() if title else None,
                "price": price,
                "price_per_sqm": price_per_sqm,
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "parking_spaces": parking_spaces,
                "area": area,
                "neighborhood": neighborhood,
                "city": "São José dos Campos",
                "state": "SP",
                "address": None,
                "condo_fee": condo_fee,
                "iptu": iptu,
                "total_cost": price + (condo_fee or 0) + (iptu or 0),
                "collected_at": datetime.now().isoformat(),
                "listing_date": None
            }
            
            return property_data
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados do card: {e}")
            return None
    
    async def scrape_page_with_retry(self, page: Page, page_num: int) -> Tuple[List[Dict], bool]:
        """Raspa uma página com sistema de retry"""
        for attempt in range(self.max_retries):
            try:
                properties = await self.scrape_page(page, page_num)
                
                # Detectar possível rate limiting
                if len(properties) == 0:
                    self.stats['empty_pages'] += 1
                    logger.warning(f"Página {page_num} retornou vazia - possível rate limiting?")
                    
                    if attempt < self.max_retries - 1:
                        logger.info(f"Aguardando {self.retry_delay}s antes de tentar novamente...")
                        await asyncio.sleep(self.retry_delay)
                        continue
                
                return properties, True
                
            except PlaywrightTimeout as e:
                logger.error(f"Timeout na página {page_num}, tentativa {attempt + 1}/{self.max_retries}")
                self.stats['retries'] += 1
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    self.stats['pages_failed'] += 1
                    self.stats['errors'].append(f"Página {page_num}: Timeout após {self.max_retries} tentativas")
                    return [], False
                    
            except Exception as e:
                logger.error(f"Erro na página {page_num}, tentativa {attempt + 1}/{self.max_retries}: {e}")
                self.stats['retries'] += 1
                
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    self.stats['pages_failed'] += 1
                    self.stats['errors'].append(f"Página {page_num}: {str(e)}")
                    return [], False
        
        return [], False
    
    async def scrape_page(self, page: Page, page_num: int) -> List[Dict]:
        """Raspa uma página de resultados"""
        # Construir URL com paginação
        # OLX usa &o=1 para primeira página quando tem filtros, ou nada se for URL base
        url = f"{self.base_url}{self.filters}"
        if page_num == 1:
            # Primeira página não precisa do parâmetro &o
            pass
        else:
            # Páginas seguintes usam &o=2, &o=3, etc
            url += f"&o={page_num}"
        
        logger.info(f"Acessando página {page_num}: {url}")
        
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
            if i % 5 == 0 and i > 0:
                await asyncio.sleep(random.uniform(0.3, 0.7))
        
        logger.info(f"Página {page_num}: {len(page_properties)} imóveis válidos extraídos")
        self.stats['pages_processed'] += 1
        
        return page_properties
    
    def calculate_stats(self) -> Dict:
        """Calcula estatísticas detalhadas dos imóveis coletados"""
        if not self.collected_properties:
            return {}
        
        # Separar por tipo
        apartments = [p for p in self.collected_properties if p.get('property_type') == 'apartamento']
        houses = [p for p in self.collected_properties if p.get('property_type') == 'casa']
        
        def get_price_stats(properties: List[Dict], field: str) -> Dict:
            """Calcula estatísticas de preço para um conjunto de imóveis"""
            prices = [p[field] for p in properties if p.get(field) is not None and p[field] > 0]
            if not prices:
                return {'min': 0, 'max': 0, 'avg': 0, 'count': 0}
            
            return {
                'min': min(prices),
                'max': max(prices),
                'avg': round(sum(prices) / len(prices), 2),
                'count': len(prices)
            }
        
        # Encontrar imóveis com valores extremos
        all_props = self.collected_properties
        
        # Preço total
        min_price_prop = min(all_props, key=lambda x: x.get('price', float('inf')))
        max_price_prop = max(all_props, key=lambda x: x.get('price', 0))
        
        # Preço por m²
        props_with_sqm = [p for p in all_props if p.get('price_per_sqm')]
        min_sqm_prop = min(props_with_sqm, key=lambda x: x['price_per_sqm']) if props_with_sqm else None
        max_sqm_prop = max(props_with_sqm, key=lambda x: x['price_per_sqm']) if props_with_sqm else None
        
        # Estatísticas por tipo
        stats = {
            'total': len(self.collected_properties),
            'by_type': {
                'apartamento': len(apartments),
                'casa': len(houses),
                'não_identificado': len([p for p in all_props if not p.get('property_type')])
            },
            'by_bedrooms': {},
            'price_stats': {
                'all': get_price_stats(all_props, 'price'),
                'apartments': get_price_stats(apartments, 'price'),
                'houses': get_price_stats(houses, 'price')
            },
            'price_per_sqm_stats': {
                'all': get_price_stats(all_props, 'price_per_sqm'),
                'apartments': get_price_stats(apartments, 'price_per_sqm'),
                'houses': get_price_stats(houses, 'price_per_sqm')
            },
            'extreme_values': {
                'min_price': {
                    'id': min_price_prop.get('id'),
                    'title': min_price_prop.get('title'),
                    'price': min_price_prop.get('price'),
                    'url': min_price_prop.get('url')
                },
                'max_price': {
                    'id': max_price_prop.get('id'),
                    'title': max_price_prop.get('title'),
                    'price': max_price_prop.get('price'),
                    'url': max_price_prop.get('url')
                },
                'min_price_per_sqm': {
                    'id': min_sqm_prop.get('id') if min_sqm_prop else None,
                    'title': min_sqm_prop.get('title') if min_sqm_prop else None,
                    'price_per_sqm': min_sqm_prop.get('price_per_sqm') if min_sqm_prop else None,
                    'url': min_sqm_prop.get('url') if min_sqm_prop else None
                },
                'max_price_per_sqm': {
                    'id': max_sqm_prop.get('id') if max_sqm_prop else None,
                    'title': max_sqm_prop.get('title') if max_sqm_prop else None,
                    'price_per_sqm': max_sqm_prop.get('price_per_sqm') if max_sqm_prop else None,
                    'url': max_sqm_prop.get('url') if max_sqm_prop else None
                }
            }
        }
        
        # Estatísticas por quartos
        for prop in all_props:
            bedrooms = str(prop.get('bedrooms', 'não_informado'))
            stats['by_bedrooms'][bedrooms] = stats['by_bedrooms'].get(bedrooms, 0) + 1
        
        return stats
    
    def save_data(self):
        """Salva dados em JSON e CSV"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Salvar JSON
        json_filename = self.data_dir / f"olx_data_v2_{timestamp}.json"
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(self.collected_properties, f, ensure_ascii=False, indent=2)
        logger.info(f"JSON salvo: {json_filename}")
        
        # Salvar CSV
        csv_filename = self.data_dir / f"olx_data_v2_{timestamp}.csv"
        if self.collected_properties:
            # Definir ordem das colunas
            fieldnames = [
                'id', 'portal', 'property_type', 'title', 'price', 'price_per_sqm',
                'bedrooms', 'bathrooms', 'parking_spaces', 'area', 'neighborhood',
                'city', 'state', 'condo_fee', 'iptu', 'total_cost', 'url', 'collected_at'
            ]
            
            with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(self.collected_properties)
            
            logger.info(f"CSV salvo: {csv_filename}")
        
        logger.info(f"Total de imóveis salvos: {len(self.collected_properties)}")
    
    def print_statistics(self):
        """Imprime estatísticas detalhadas"""
        stats = self.calculate_stats()
        
        print("\n" + "="*80)
        print("ESTATÍSTICAS FINAIS - OLX SCRAPER")
        print("="*80)
        
        # Informações de execução
        duration = datetime.now() - self.stats['start_time']
        print(f"\nEXECUÇÃO:")
        print(f"  Duração: {duration}")
        print(f"  Páginas processadas: {self.stats['pages_processed']}")
        print(f"  Páginas com erro: {self.stats['pages_failed']}")
        print(f"  Tentativas de retry: {self.stats['retries']}")
        print(f"  Páginas vazias: {self.stats['empty_pages']}")
        
        # Totais
        print(f"\nTOTAL DE IMÓVEIS: {stats.get('total', 0)}")
        
        # Por tipo
        print("\nPOR TIPO DE IMÓVEL:")
        for prop_type, count in stats.get('by_type', {}).items():
            percentage = (count / stats['total'] * 100) if stats['total'] > 0 else 0
            print(f"  {prop_type}: {count} ({percentage:.1f}%)")
        
        # Por quartos
        print("\nPOR NÚMERO DE QUARTOS:")
        by_bedrooms = stats.get('by_bedrooms', {})
        for bedrooms in sorted(by_bedrooms.keys(), key=lambda x: (x != 'não_informado', x)):
            count = by_bedrooms[bedrooms]
            percentage = (count / stats['total'] * 100) if stats['total'] > 0 else 0
            print(f"  {bedrooms} quartos: {count} ({percentage:.1f}%)")
        
        # Estatísticas de preço
        print("\nESTATÍSTICAS DE PREÇO (R$):")
        price_stats = stats.get('price_stats', {})
        for category in ['all', 'apartments', 'houses']:
            if category in price_stats and price_stats[category]['count'] > 0:
                cat_stats = price_stats[category]
                label = {'all': 'Todos', 'apartments': 'Apartamentos', 'houses': 'Casas'}[category]
                print(f"\n  {label}:")
                print(f"    Mínimo: R$ {cat_stats['min']:,.2f}")
                print(f"    Máximo: R$ {cat_stats['max']:,.2f}")
                print(f"    Médio: R$ {cat_stats['avg']:,.2f}")
                print(f"    Quantidade: {cat_stats['count']}")
        
        # Estatísticas de preço por m²
        print("\nESTATÍSTICAS DE PREÇO POR M² (R$/m²):")
        sqm_stats = stats.get('price_per_sqm_stats', {})
        for category in ['all', 'apartments', 'houses']:
            if category in sqm_stats and sqm_stats[category]['count'] > 0:
                cat_stats = sqm_stats[category]
                label = {'all': 'Todos', 'apartments': 'Apartamentos', 'houses': 'Casas'}[category]
                print(f"\n  {label}:")
                print(f"    Mínimo: R$ {cat_stats['min']:,.2f}/m²")
                print(f"    Máximo: R$ {cat_stats['max']:,.2f}/m²")
                print(f"    Médio: R$ {cat_stats['avg']:,.2f}/m²")
                print(f"    Quantidade: {cat_stats['count']}")
        
        # Valores extremos
        print("\nIMÓVEIS COM VALORES EXTREMOS:")
        extremes = stats.get('extreme_values', {})
        
        if extremes.get('min_price', {}).get('id'):
            print(f"\n  Menor preço total:")
            print(f"    {extremes['min_price']['title'][:60]}...")
            print(f"    R$ {extremes['min_price']['price']:,.2f}")
            print(f"    {extremes['min_price']['url']}")
        
        if extremes.get('max_price', {}).get('id'):
            print(f"\n  Maior preço total:")
            print(f"    {extremes['max_price']['title'][:60]}...")
            print(f"    R$ {extremes['max_price']['price']:,.2f}")
            print(f"    {extremes['max_price']['url']}")
        
        if extremes.get('min_price_per_sqm', {}).get('id'):
            print(f"\n  Menor preço por m²:")
            print(f"    {extremes['min_price_per_sqm']['title'][:60]}...")
            print(f"    R$ {extremes['min_price_per_sqm']['price_per_sqm']:,.2f}/m²")
            print(f"    {extremes['min_price_per_sqm']['url']}")
        
        if extremes.get('max_price_per_sqm', {}).get('id'):
            print(f"\n  Maior preço por m²:")
            print(f"    {extremes['max_price_per_sqm']['title'][:60]}...")
            print(f"    R$ {extremes['max_price_per_sqm']['price_per_sqm']:,.2f}/m²")
            print(f"    {extremes['max_price_per_sqm']['url']}")
        
        # Erros
        if self.stats['errors']:
            print(f"\nERROS ENCONTRADOS ({len(self.stats['errors'])}):")
            for error in self.stats['errors'][:5]:  # Mostrar apenas os 5 primeiros
                print(f"  - {error}")
            if len(self.stats['errors']) > 5:
                print(f"  ... e mais {len(self.stats['errors']) - 5} erros")
        
        print("\n" + "="*80 + "\n")
    
    def print_progress(self, current_page: int, total_pages: int):
        """Imprime progresso em tempo real"""
        collected = len(self.collected_properties)
        percentage = (current_page / total_pages * 100) if total_pages > 0 else 0
        
        print(f"\n📊 PROGRESSO: Página {current_page}/{total_pages} ({percentage:.1f}%)")
        print(f"   Imóveis coletados: {collected}")
        print(f"   Taxa de sucesso: {(self.stats['pages_processed'] / current_page * 100) if current_page > 0 else 0:.1f}%")
        print(f"   Tempo decorrido: {datetime.now() - self.stats['start_time']}")
        
        # Estimativa de conclusão
        if current_page > 0:
            elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
            avg_time_per_page = elapsed / current_page
            remaining_pages = total_pages - current_page
            eta_seconds = remaining_pages * avg_time_per_page
            eta = datetime.now() + timedelta(seconds=eta_seconds)
            print(f"   Conclusão estimada: {eta.strftime('%H:%M:%S')}")
    
    async def run(self, target_pages: int = None):
        """Executa o scraper completo"""
        logger.info("="*60)
        logger.info("OLX SCRAPER v2 - EXECUÇÃO COMPLETA")
        logger.info("="*60)
        
        # Carregar checkpoint
        checkpoint = self.load_checkpoint()
        start_page = checkpoint['last_page'] + 1 if checkpoint['last_page'] > 0 else 1
        
        # Determinar número de páginas
        if target_pages is None:
            # Estimar ~80 páginas para 4000 imóveis (50 por página)
            target_pages = 80
        
        logger.info(f"Meta: coletar ~{target_pages * 50} imóveis ({target_pages} páginas)")
        
        if start_page > 1:
            logger.info(f"Continuando da página {start_page}")
        
        async with async_playwright() as p:
            # Iniciar navegador
            browser = await p.chromium.launch(
                headless=False,  # Visível para validação
                args=['--disable-blink-features=AutomationControlled']
            )
            
            # Criar contexto
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1366, 'height': 768}
            )
            
            page = await context.new_page()
            
            try:
                # Coletar páginas
                for page_num in range(start_page, target_pages + 1):
                    # Progresso
                    self.print_progress(page_num, target_pages)
                    
                    # Raspar página com retry
                    page_properties, success = await self.scrape_page_with_retry(page, page_num)
                    
                    if success and page_properties:
                        # Adicionar aos dados coletados
                        self.collected_properties.extend(page_properties)
                        
                        # Salvar checkpoint
                        self.save_checkpoint(page_num)
                        
                        # Salvar dados a cada página (~50 imóveis)
                        if page_num % 1 == 0:  # A cada página
                            self.save_data()
                    
                    # Se muitas páginas vazias consecutivas, parar
                    if self.stats['empty_pages'] >= 3:
                        logger.warning("3 páginas vazias consecutivas - possível fim dos resultados ou rate limiting")
                        break
                    
                    # Delay entre páginas
                    if page_num < target_pages:
                        delay = random.uniform(5, 10)
                        logger.info(f"Aguardando {delay:.1f}s antes da próxima página...")
                        await asyncio.sleep(delay)
                
                # Salvar dados finais
                self.save_data()
                
                # Salvar checkpoint final
                self.save_checkpoint(page_num)
                
                # Imprimir estatísticas
                self.print_statistics()
                
            except KeyboardInterrupt:
                logger.info("\nInterrompido pelo usuário - salvando dados...")
                self.save_data()
                self.save_checkpoint(page_num)
                self.print_statistics()
                
            except Exception as e:
                logger.error(f"Erro durante execução: {e}")
                # Salvar o que foi coletado
                if self.collected_properties:
                    self.save_data()
                    self.save_checkpoint(page_num)
                raise
            
            finally:
                await context.close()
                await browser.close()
        
        logger.info("="*60)
        logger.info("SCRAPER FINALIZADO!")
        logger.info(f"Arquivos salvos em: {self.data_dir}")
        logger.info("="*60)

if __name__ == "__main__":
    import sys
    
    # Criar scraper
    scraper = OLXScraper()
    
    # Verificar se deve limpar checkpoint
    if len(sys.argv) > 1 and sys.argv[1] == '--reset':
        print("Limpando checkpoint e começando do zero...")
        if scraper.checkpoint_file.exists():
            scraper.checkpoint_file.unlink()
        print("Checkpoint removido!")
    
    # Executar coleta completa
    # Para testar com menos páginas, use: scraper.run(target_pages=10)
    asyncio.run(scraper.run())