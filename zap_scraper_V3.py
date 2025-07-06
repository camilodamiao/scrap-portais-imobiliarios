import asyncio
import json
import re
import os
from datetime import datetime
from playwright.async_api import async_playwright
from typing import Dict, List, Optional, Tuple
import logging
import random
import hashlib
from pathlib import Path

# Configurar logging detalhado
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler('zap_scraper_v3.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ZapScraperV3:
    """Scraper de produção v3 para Zap Imóveis com seletores data-cy validados"""
    
    def __init__(self, checkpoint_dir: str = "checkpoints"):
        self.base_url = "https://www.zapimoveis.com.br"
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)
        self.data = []
        self.processed_ids = set()
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.save_interval = 100  # Salvar a cada 100 imóveis
        
        # Bairros alvo
        self.target_neighborhoods = [
            'Vila Adyana', 'Vila Ema', 'Jardim Aquarius', 
            'Jardim Esplanada', 'Jardim das Colinas', 
            'Jardim Apolo', 'Urbanova', 'Jardim das Indústrias'
        ]
        
    def clean_price_text(self, text: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Extrai e limpa valores de preço, condomínio e IPTU"""
        rent = None
        condo_fee = None
        iptu = None
        
        # Patterns para cada tipo de valor
        patterns = {
            'rent': r'R\$\s*([\d\.]+)(?:/mês)?',
            'condo': r'Condomínio:\s*R\$\s*([\d\.]+)',
            'iptu': r'IPTU:\s*R\$\s*([\d\.]+)'
        }
        
        # Extrair aluguel
        rent_match = re.search(patterns['rent'], text)
        if rent_match:
            rent = float(rent_match.group(1).replace('.', ''))
        
        # Extrair condomínio
        condo_match = re.search(patterns['condo'], text)
        if condo_match:
            condo_fee = float(condo_match.group(1).replace('.', ''))
            
        # Extrair IPTU
        iptu_match = re.search(patterns['iptu'], text)
        if iptu_match:
            iptu = float(iptu_match.group(1).replace('.', ''))
            
        return rent, condo_fee, iptu
    
    def clean_numeric_text(self, text: str, unit: str = '') -> Optional[int]:
        """Extrai número de texto com unidade"""
        # Remove a unidade e textos desnecessários
        clean_text = text.replace(unit, '').strip()
        # Extrai apenas números
        match = re.search(r'(\d+)', clean_text)
        return int(match.group(1)) if match else None
    
    def load_checkpoint(self) -> Dict:
        """Carrega o último checkpoint se existir"""
        checkpoint_file = self.checkpoint_dir / "latest_checkpoint.json"
        
        if checkpoint_file.exists():
            try:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)
                    logger.info(f"✅ Checkpoint carregado: {checkpoint['total_collected']} imóveis já coletados")
                    logger.info(f"📄 Última página processada: {checkpoint.get('last_page', 0)}")
                    return checkpoint
            except Exception as e:
                logger.error(f"❌ Erro ao carregar checkpoint: {e}")
        
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
        
        logger.info(f"💾 Checkpoint salvo: Página {page_num}, Total: {len(self.data)} imóveis")
    
    def save_incremental_data(self):
        """Salva dados incrementalmente"""
        output_file = f"zap_data_v3_{self.session_id}.json"
        
        # Estatísticas básicas
        stats = self.calculate_statistics()
        
        output = {
            'metadata': {
                'portal': 'zap_imoveis',
                'session_id': self.session_id,
                'total_listings': len(self.data),
                'last_update': datetime.now().isoformat(),
                'filters': {
                    'location': 'São José dos Campos - SP',
                    'neighborhoods': self.target_neighborhoods,
                    'min_bedrooms': 3,
                    'min_parking': 2,
                    'type': 'RENTAL'
                },
                'statistics': stats
            },
            'listings': self.data
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        logger.info(f"📁 Dados salvos em {output_file}")
        
        # Backup incremental
        backup_file = self.checkpoint_dir / f"backup_{self.session_id}_{len(self.data)}.json"
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False)
    
    async def extract_listing_from_card(self, card) -> Optional[Dict]:
        """Extrai dados de um card usando seletores data-cy validados"""
        try:
            listing = {
                'portal': 'zap_imoveis',
                'collected_at': datetime.now().isoformat()
            }
            
            # Link e ID
            link_element = await card.query_selector('a')
            if link_element:
                href = await link_element.get_attribute('href')
                listing['url'] = href if href.startswith('http') else f"{self.base_url}{href}"
                
                # Extrair ID do link
                id_match = re.search(r'id-(\d+)', href)
                if id_match:
                    listing['id'] = id_match.group(1)
                else:
                    # Gerar ID único baseado em características
                    listing['id'] = hashlib.md5(href.encode()).hexdigest()[:12]
            
            # Localização (bairro)
            location_elem = await card.query_selector('[data-cy="rp-cardProperty-location-txt"]')
            if location_elem:
                location_text = await location_elem.inner_text()
                listing['neighborhood'] = location_text.strip()
                listing['city'] = 'São José dos Campos'
                listing['state'] = 'SP'
            
            # Rua
            street_elem = await card.query_selector('[data-cy="rp-cardProperty-street-txt"]')
            if street_elem:
                listing['street'] = await street_elem.inner_text()
                listing['address'] = f"{listing.get('street', '')}, {listing.get('neighborhood', '')}"
            
            # Área
            area_elem = await card.query_selector('[data-cy="rp-cardProperty-propertyArea-txt"]')
            if area_elem:
                area_text = await area_elem.inner_text()
                listing['area'] = self.clean_numeric_text(area_text, 'm²')
            
            # Quartos
            bedrooms_elem = await card.query_selector('[data-cy="rp-cardProperty-bedroomQuantity-txt"]')
            if bedrooms_elem:
                bedrooms_text = await bedrooms_elem.inner_text()
                listing['bedrooms'] = self.clean_numeric_text(bedrooms_text)
            
            # Banheiros
            bathrooms_elem = await card.query_selector('[data-cy="rp-cardProperty-bathroomQuantity-txt"]')
            if bathrooms_elem:
                bathrooms_text = await bathrooms_elem.inner_text()
                listing['bathrooms'] = self.clean_numeric_text(bathrooms_text)
            
            # Vagas
            parking_elem = await card.query_selector('[data-cy="rp-cardProperty-parkingSpacesQuantity-txt"]')
            if parking_elem:
                parking_text = await parking_elem.inner_text()
                listing['parking_spaces'] = self.clean_numeric_text(parking_text)
            
            # Preço, condomínio e IPTU
            price_elem = await card.query_selector('[data-cy="rp-cardProperty-price-txt"]')
            if price_elem:
                price_text = await price_elem.inner_text()
                rent, condo_fee, iptu = self.clean_price_text(price_text)
                
                if rent:
                    listing['price'] = rent
                    listing['price_type'] = 'RENTAL'
                if condo_fee:
                    listing['condo_fee'] = condo_fee
                if iptu:
                    listing['iptu'] = iptu
                    
                # Guardar texto original para debug
                listing['price_raw'] = price_text
            
            # Validar dados mínimos
            if 'id' in listing and 'price' in listing:
                return listing
            else:
                logger.warning(f"Listagem incompleta - faltam campos obrigatórios")
                return None
                
        except Exception as e:
            logger.error(f"Erro ao extrair dados do card: {str(e)}")
            return None
    
    async def scrape_page(self, page, page_num: int) -> List[Dict]:
        """Scraping de uma página específica"""
        # Construir URL com paginação
        if page_num == 1:
            url = ("https://www.zapimoveis.com.br/aluguel/imoveis/sp+sao-jose-dos-campos/"
                   "3-quartos/?onde=%2CS%C3%A3o+Paulo%2CS%C3%A3o+Jos%C3%A9+dos+Campos%2C%2C%2C%2C%2Ccity"
                   "%2CBR%3ESao+Paulo%3ENULL%3ESao+Jose+dos+Campos%2C-23.21984%2C-45.891566%2C&"
                   "quartos=3%2C4&vagas=2&transacao=aluguel")
        else:
            url = ("https://www.zapimoveis.com.br/aluguel/imoveis/sp+sao-jose-dos-campos/"
                   f"3-quartos/?pagina={page_num}&onde=%2CS%C3%A3o+Paulo%2CS%C3%A3o+Jos%C3%A9+dos+Campos"
                   "%2C%2C%2C%2C%2Ccity%2CBR%3ESao+Paulo%3ENULL%3ESao+Jose+dos+Campos%2C-23.21984"
                   "%2C-45.891566%2C&quartos=3%2C4&vagas=2&transacao=aluguel")
        
        logger.info(f"🌐 Acessando página {page_num}...")
        
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            
            # Aguardar carregamento dos cards
            await page.wait_for_selector('li[data-cy="rp-property-cd"]', timeout=10000)
            await asyncio.sleep(random.uniform(2, 4))
            
            # Scroll para garantir carregamento completo
            for i in range(2):
                await page.evaluate('window.scrollBy(0, window.innerHeight)')
                await asyncio.sleep(1)
            
            # Buscar todos os cards
            cards = await page.query_selector_all('li[data-cy="rp-property-cd"]')
            logger.info(f"📦 Página {page_num}: {len(cards)} cards encontrados")
            
            listings_data = []
            
            for i, card in enumerate(cards):
                try:
                    listing = await self.extract_listing_from_card(card)
                    
                    if listing and listing['id'] not in self.processed_ids:
                        # Filtrar por bairros alvo (se especificado)
                        neighborhood = listing.get('neighborhood', '')
                        if not self.target_neighborhoods or any(target in neighborhood for target in self.target_neighborhoods):
                            listings_data.append(listing)
                            self.processed_ids.add(listing['id'])
                            
                            # Log de progresso a cada 10 cards
                            if (i + 1) % 10 == 0:
                                logger.info(f"  Processados {i + 1}/{len(cards)} cards")
                                
                except Exception as e:
                    logger.error(f"Erro no card {i + 1}: {str(e)}")
                    continue
            
            logger.info(f"✅ Página {page_num}: {len(listings_data)} novas listagens válidas extraídas")
            return listings_data
            
        except Exception as e:
            logger.error(f"❌ Erro na página {page_num}: {str(e)}")
            return []
    
    def calculate_statistics(self) -> Dict:
        """Calcula estatísticas dos dados coletados"""
        if not self.data:
            return {}
            
        stats = {
            'total': len(self.data),
            'by_neighborhood': {},
            'price_stats': {},
            'area_stats': {},
            'rooms_distribution': {}
        }
        
        # Coletar valores
        prices = []
        areas = []
        neighborhoods = {}
        rooms = {}
        
        for listing in self.data:
            # Preços
            if 'price' in listing:
                prices.append(listing['price'])
            
            # Áreas
            if 'area' in listing:
                areas.append(listing['area'])
            
            # Bairros
            neighborhood = listing.get('neighborhood', 'N/A')
            neighborhoods[neighborhood] = neighborhoods.get(neighborhood, 0) + 1
            
            # Quartos
            bedrooms = listing.get('bedrooms', 0)
            rooms[bedrooms] = rooms.get(bedrooms, 0) + 1
        
        # Calcular estatísticas
        if prices:
            stats['price_stats'] = {
                'avg': sum(prices) / len(prices),
                'min': min(prices),
                'max': max(prices),
                'count': len(prices)
            }
        
        if areas:
            stats['area_stats'] = {
                'avg': sum(areas) / len(areas),
                'min': min(areas),
                'max': max(areas),
                'count': len(areas)
            }
        
        stats['by_neighborhood'] = neighborhoods
        stats['rooms_distribution'] = rooms
        
        return stats
    
    async def run_scraper(self, max_pages: Optional[int] = None):
        """Execução principal do scraper"""
        logger.info("=== INICIANDO ZAP SCRAPER V3 ===")
        logger.info(f"📅 Sessão ID: {self.session_id}")
        logger.info(f"🎯 Meta: ~3.500 imóveis com filtros aplicados")
        
        # Carregar checkpoint
        checkpoint = self.load_checkpoint()
        start_page = checkpoint['last_page'] + 1
        
        # Restaurar dados anteriores
        if checkpoint['processed_ids']:
            self.processed_ids = set(checkpoint['processed_ids'])
            # Tentar carregar dados da sessão anterior
            prev_file = f"zap_data_v3_{checkpoint['session_id']}.json"
            if os.path.exists(prev_file):
                with open(prev_file, 'r', encoding='utf-8') as f:
                    prev_data = json.load(f)
                    self.data = prev_data['listings']
                    self.session_id = checkpoint['session_id']  # Manter mesmo ID
                    logger.info(f"📂 Dados anteriores restaurados: {len(self.data)} imóveis")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,  # Visível para passar Cloudflare
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-features=IsolateOrigins,site-per-process'
                ]
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='pt-BR',
                timezone_id='America/Sao_Paulo'
            )
            
            # Script anti-detecção
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)
            
            page = await context.new_page()
            
            # Loop principal
            page_num = start_page
            consecutive_empty_pages = 0
            max_empty_pages = 3
            last_save_count = len(self.data)
            
            # Definir limite de páginas
            total_pages = max_pages if max_pages else 150  # Estimar ~150 páginas para 3.500 imóveis
            
            while page_num <= total_pages and consecutive_empty_pages < max_empty_pages:
                try:
                    # Scraping da página
                    listings = await self.scrape_page(page, page_num)
                    
                    if listings:
                        self.data.extend(listings)
                        consecutive_empty_pages = 0
                        
                        # Salvar incrementalmente
                        if len(self.data) - last_save_count >= self.save_interval:
                            self.save_incremental_data()
                            self.save_checkpoint(page_num)
                            last_save_count = len(self.data)
                            
                            # Mostrar progresso
                            stats = self.calculate_statistics()
                            logger.info(f"📊 Progresso: {len(self.data)} imóveis")
                            if 'price_stats' in stats:
                                logger.info(f"   💰 Preço médio: R$ {stats['price_stats']['avg']:,.2f}")
                        
                        # Verificar se atingiu a meta
                        if len(self.data) >= 3500:
                            logger.info(f"🎯 Meta atingida! {len(self.data)} imóveis coletados")
                            break
                    else:
                        consecutive_empty_pages += 1
                        logger.warning(f"⚠️ Página {page_num} sem dados. Vazias consecutivas: {consecutive_empty_pages}")
                    
                    # Salvar checkpoint após cada página
                    self.save_checkpoint(page_num)
                    
                    # Delay entre páginas
                    delay = random.uniform(3, 6)
                    logger.info(f"⏳ Aguardando {delay:.1f}s antes da próxima página...")
                    await asyncio.sleep(delay)
                    
                    page_num += 1
                    
                except Exception as e:
                    logger.error(f"❌ Erro crítico na página {page_num}: {str(e)}")
                    # Salvar estado atual antes de parar
                    self.save_incremental_data()
                    self.save_checkpoint(page_num - 1)
                    
                    # Tentar continuar se não for erro fatal
                    if "TimeoutError" not in str(e):
                        break
                    else:
                        logger.info("🔄 Tentando continuar após timeout...")
                        page_num += 1
            
            await browser.close()
        
        # Salvar dados finais
        self.save_incremental_data()
        
        # Limpar checkpoint se completou com sucesso
        if consecutive_empty_pages >= max_empty_pages or (max_pages and page_num > max_pages):
            self.cleanup_checkpoint()
        
        # Relatório final
        self.print_final_report()
    
    def cleanup_checkpoint(self):
        """Move checkpoint para histórico após conclusão"""
        checkpoint_file = self.checkpoint_dir / "latest_checkpoint.json"
        if checkpoint_file.exists():
            history_file = self.checkpoint_dir / f"completed_{self.session_id}.json"
            checkpoint_file.rename(history_file)
            logger.info("✅ Checkpoint arquivado no histórico")
    
    def print_final_report(self):
        """Imprime relatório final detalhado"""
        logger.info("\n" + "="*50)
        logger.info("📊 RELATÓRIO FINAL - ZAP SCRAPER V3")
        logger.info("="*50)
        
        stats = self.calculate_statistics()
        
        logger.info(f"\n📈 RESUMO GERAL:")
        logger.info(f"   Total de imóveis: {len(self.data)}")
        logger.info(f"   IDs únicos: {len(self.processed_ids)}")
        
        if 'price_stats' in stats:
            logger.info(f"\n💰 ESTATÍSTICAS DE PREÇO:")
            logger.info(f"   Média: R$ {stats['price_stats']['avg']:,.2f}")
            logger.info(f"   Mínimo: R$ {stats['price_stats']['min']:,.2f}")
            logger.info(f"   Máximo: R$ {stats['price_stats']['max']:,.2f}")
        
        if 'area_stats' in stats:
            logger.info(f"\n📐 ESTATÍSTICAS DE ÁREA:")
            logger.info(f"   Média: {stats['area_stats']['avg']:.1f} m²")
            logger.info(f"   Mínima: {stats['area_stats']['min']} m²")
            logger.info(f"   Máxima: {stats['area_stats']['max']} m²")
        
        if stats.get('by_neighborhood'):
            logger.info(f"\n🏘️ TOP 10 BAIRROS:")
            sorted_neighborhoods = sorted(
                stats['by_neighborhood'].items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:10]
            for neighborhood, count in sorted_neighborhoods:
                logger.info(f"   {neighborhood}: {count} imóveis")
        
        if stats.get('rooms_distribution'):
            logger.info(f"\n🛏️ DISTRIBUIÇÃO POR QUARTOS:")
            for rooms, count in sorted(stats['rooms_distribution'].items()):
                logger.info(f"   {rooms} quartos: {count} imóveis")
        
        logger.info(f"\n📁 Dados salvos em: zap_data_v3_{self.session_id}.json")
        logger.info("="*50)

async def main():
    """Função principal"""
    scraper = ZapScraperV3()
    
    # Executar scraper
    # Para teste, limitar a 5 páginas. Para produção, remover o parâmetro max_pages
    await scraper.run_scraper(max_pages=5)  # REMOVER max_pages para coletar tudo

if __name__ == "__main__":
    asyncio.run(main())