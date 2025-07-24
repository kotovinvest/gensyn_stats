import requests
import json
import logging
import time
import pandas as pd
from typing import Dict, List, Optional
from web3 import Web3
from datetime import datetime
from config import Config

logger = logging.getLogger(__name__)

class GensynDataCollector:
    def __init__(self):
        self.peer_api_url = Config.GENSYN_PEER_API_URL
        self.rpc_url = Config.GENSYN_RPC_URL
        self.contract_address = Config.GENSYN_CONTRACT_ADDRESS
        self.chain_id = Config.GENSYN_CHAIN_ID
        
        # Настройка прокси
        self.proxies = self.setup_proxy()
        
        # Настройка Web3 подключения
        self.setup_web3_connection()
        
        self.contract_abi = [
            {
                "inputs": [{"internalType": "string[]", "name": "peerIds", "type": "string[]"}],
                "name": "getEoa",
                "outputs": [{"internalType": "address[]", "name": "", "type": "address[]"}],
                "stateMutability": "view",
                "type": "function"
            }
        ]
        
        try:
            self.contract = self.w3.eth.contract(
                address=self.contract_address,
                abi=self.contract_abi
            )
            logger.info(f"✓ Контракт инициализирован: {self.contract_address}")
        except Exception as e:
            logger.error(f"✗ Ошибка инициализации контракта: {e}")
            self.contract = None
        
        # Настройка сессии для API запросов
        self.session = requests.Session()
        if self.proxies:
            self.session.proxies.update(self.proxies)
            logger.info("✓ Requests session настроен с прокси")
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def setup_proxy(self) -> Optional[Dict[str, str]]:
        """Настройка прокси в формате log:pass@ip:port"""
        try:
            if not Config.PROXY:
                logger.info("✓ Прокси не настроен")
                return None
            
            # Парсинг формата log:pass@ip:port
            if '@' not in Config.PROXY:
                logger.error("❌ Неверный формат прокси! Используйте: login:password@ip:port")
                return None
            
            auth_part, server_part = Config.PROXY.split('@')
            
            if ':' not in auth_part or ':' not in server_part:
                logger.error("❌ Неверный формат прокси! Используйте: login:password@ip:port")
                return None
            
            login, password = auth_part.split(':', 1)
            ip, port = server_part.split(':', 1)
            
            proxy_url = f"http://{login}:{password}@{ip}:{port}"
            
            proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            
            logger.info(f"✓ Прокси настроен: {login}:***@{ip}:{port}")
            return proxies
            
        except Exception as e:
            logger.error(f"❌ Ошибка настройки прокси: {e}")
            return None
    
    def setup_web3_connection(self):
        """Настройка Web3 подключения с поддержкой прокси"""
        try:
            # Настройка провайдера с прокси
            if self.proxies:
                # Настройка Web3 с прокси
                from web3.providers import HTTPProvider
                
                provider = HTTPProvider(
                    endpoint_uri=self.rpc_url,
                    request_kwargs={
                        'timeout': 120,
                        'proxies': self.proxies
                    }
                )
                
                self.w3 = Web3(provider)
                logger.info(f"✓ Web3 настроен с прокси для Gensyn Testnet")
            else:
                # Стандартное подключение без прокси
                self.w3 = Web3(Web3.HTTPProvider(
                    self.rpc_url,
                    request_kwargs={'timeout': 120}
                ))
                logger.info(f"✓ Web3 настроен без прокси для Gensyn Testnet")
            
            # Тест подключения
            if self.w3.is_connected():
                logger.info(f"✓ Подключение к Gensyn Testnet (Chain ID: {self.chain_id})")
                
                # Дополнительный тест подключения
                try:
                    latest_block = self.w3.eth.get_block('latest')
                    logger.info(f"✓ Последний блок: {latest_block['number']}")
                except Exception as e:
                    logger.warning(f"⚠️ Предупреждение при получении блока: {e}")
            else:
                logger.error("✗ Не удалось подключиться к Gensyn Testnet")
                
        except Exception as e:
            logger.error(f"✗ Ошибка подключения к Web3: {e}")
            # Резервное подключение без прокси
            try:
                self.w3 = Web3(Web3.HTTPProvider(
                    self.rpc_url,
                    request_kwargs={'timeout': 60}
                ))
                logger.info("✓ Резервное подключение без прокси")
            except Exception as fallback_error:
                logger.error(f"✗ Критическая ошибка подключения: {fallback_error}")
                self.w3 = None
    
    def read_nodes_data(self, filename: str) -> List[Dict]:
        """Читает данные о нодах из Excel файла"""
        try:
            # Читаем Excel файл
            df = pd.read_excel(filename)
            
            # Проверяем наличие необходимых колонок
            required_columns = ['Name', 'ID', 'Type']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"❌ Отсутствуют обязательные колонки в файле {filename}: {missing_columns}")
                logger.error(f"Найденные колонки: {list(df.columns)}")
                return []
            
            # Очищаем данные от пустых строк
            df = df.dropna(subset=['ID'])
            
            # Преобразуем в список словарей
            nodes_data = []
            for _, row in df.iterrows():
                node_data = {
                    'custom_name': str(row['Name']).strip(),
                    'node_id': str(row['ID']).strip(),
                    'hardware_type': str(row['Type']).strip().upper()
                }
                nodes_data.append(node_data)
            
            logger.info(f"📋 Прочитано {len(nodes_data)} нод из файла {filename}")
            
            # Выводим краткую сводку по типам железа
            type_counts = {}
            for node in nodes_data:
                hw_type = node['hardware_type']
                type_counts[hw_type] = type_counts.get(hw_type, 0) + 1
            
            logger.info(f"📊 Распределение по типам железа: {dict(type_counts)}")
            
            return nodes_data
            
        except FileNotFoundError:
            logger.error(f"❌ Файл {filename} не найден")
            logger.info(f"💡 Создайте Excel файл {filename} с колонками: Name, ID, Type")
            return []
        except Exception as e:
            logger.error(f"❌ Ошибка при чтении файла {filename}: {e}")
            return []
    
    def get_peer_info(self, node_id: str) -> Optional[Dict]:
        try:
            params = {'id': node_id}
            response = self.session.get(self.peer_api_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"✓ Peer info для {node_id}: {data.get('peerName', 'Unknown')}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Ошибка при запросе информации о ноде {node_id}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга JSON для ноды {node_id}: {e}")
            return None
    
    def get_eoa_addresses_batch(self, node_ids: List[str]) -> Dict[str, str]:
        try:
            if not self.contract or not self.w3:
                logger.error("❌ Контракт или Web3 не инициализирован")
                return {}
            
            if not self.w3.is_connected():
                logger.error("❌ Нет подключения к Web3")
                # Попытка переподключения
                self.setup_web3_connection()
                if not self.w3 or not self.w3.is_connected():
                    return {}
            
            logger.info(f"🔗 Получаем EOA адреса для {len(node_ids)} нод...")
            
            # Попытки с увеличенным таймаутом и повторами
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    logger.info(f"  Попытка {attempt + 1}/{max_retries}...")
                    eoa_addresses = self.contract.functions.getEoa(node_ids).call({
                        'timeout': 180
                    })
                    logger.info(f"  ✓ Получен ответ от контракта")
                    break
                except Exception as e:
                    logger.warning(f"  ❌ Попытка {attempt + 1}/{max_retries} неудачна: {e}")
                    if attempt < max_retries - 1:
                        logger.info(f"  ⏳ Ожидание 10 секунд перед повтором...")
                        time.sleep(10)
                        # Попытка переподключения
                        self.setup_web3_connection()
                        if self.w3 and self.w3.is_connected():
                            self.contract = self.w3.eth.contract(
                                address=self.contract_address,
                                abi=self.contract_abi
                            )
                            logger.info(f"  ✓ Переподключение выполнено")
                    else:
                        logger.error(f"  ❌ Все попытки исчерпаны")
                        raise e
            
            result = {}
            for i in range(len(node_ids)):
                node_id = node_ids[i]
                
                if i < len(eoa_addresses):
                    eoa_address = eoa_addresses[i]
                    if eoa_address != "0x0000000000000000000000000000000000000000":
                        result[node_id] = eoa_address
                    else:
                        result[node_id] = None
                else:
                    result[node_id] = None
            
            valid_addresses = sum(1 for addr in result.values() if addr is not None)
            logger.info(f"✓ Получено {valid_addresses}/{len(node_ids)} валидных EOA адресов")
            return result
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении EOA адресов: {e}")
            return {node_id: None for node_id in node_ids}
    
    def get_last_internal_tx_time(self, eoa_address: str) -> Optional[int]:
        try:
            if not eoa_address or eoa_address == "0x0000000000000000000000000000000000000000":
                return None
            
            api_endpoints = [
                f"https://gensyn-testnet.explorer.alchemy.com/api/v2/addresses/{eoa_address}/internal-transactions",
                f"https://gensyn-testnet.explorer.alchemy.com/api/v1/addresses/{eoa_address}/internal-transactions",
                f"https://gensyn-testnet.explorer.alchemy.com/api/addresses/{eoa_address}/internal-transactions",
                f"https://gensyn-testnet.explorer.alchemy.com/api/v2/addresses/{eoa_address}/internal_transactions", 
                f"https://gensyn-testnet.explorer.alchemy.com/api/v1/addresses/{eoa_address}/internal_transactions",
                f"https://gensyn-testnet.explorer.alchemy.com/api/v2/addresses/{eoa_address}/transactions?filter=internal",
            ]
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': f'https://gensyn-testnet.explorer.alchemy.com/address/{eoa_address}?tab=internal_txns',
                'Origin': 'https://gensyn-testnet.explorer.alchemy.com'
            }
            
            for endpoint in api_endpoints:
                try:
                    response = self.session.get(endpoint, headers=headers, timeout=15)
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            
                            transactions = []
                            if isinstance(data, list):
                                transactions = data
                            elif isinstance(data, dict):
                                possible_keys = ['items', 'transactions', 'data', 'result', 'internal_transactions']
                                for key in possible_keys:
                                    if key in data and isinstance(data[key], list):
                                        transactions = data[key]
                                        break
                            
                            if transactions:
                                latest_timestamp = None
                                for tx in transactions:
                                    timestamp_fields = ['timestamp', 'block_timestamp', 'created_at', 'time', 'block_time']
                                    
                                    for field in timestamp_fields:
                                        if field in tx and tx[field]:
                                            timestamp_str = str(tx[field])
                                            try:
                                                if timestamp_str.isdigit():
                                                    timestamp = int(timestamp_str)
                                                    if timestamp > 1000000000000:
                                                        timestamp = timestamp / 1000
                                                else:
                                                    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                                                    timestamp = dt.timestamp()
                                                
                                                if latest_timestamp is None or timestamp > latest_timestamp:
                                                    latest_timestamp = timestamp
                                                    
                                                break
                                            except:
                                                continue
                                
                                if latest_timestamp:
                                    current_time = time.time()
                                    minutes_ago = int((current_time - latest_timestamp) / 60)
                                    return minutes_ago
                                    
                        except json.JSONDecodeError:
                            continue
                        
                except requests.exceptions.RequestException:
                    continue
            
            return None
            
        except Exception as e:
            logger.debug(f"Ошибка получения tx времени для {eoa_address}: {e}")
            return None
    
    def collect_node_data(self, nodes_data: List[Dict]) -> List[Dict]:
        """Собирает данные о нодах, используя информацию из Excel файла"""
        logger.info(f"🔄 Начинаем сбор данных для {len(nodes_data)} нод...")
        
        results = []
        node_ids = [node['node_id'] for node in nodes_data]
        eoa_addresses = self.get_eoa_addresses_batch(node_ids)
        
        for i, node_data in enumerate(nodes_data):
            node_id = node_data['node_id']
            custom_name = node_data['custom_name']
            hardware_type = node_data['hardware_type']
            
            logger.info(f"  📊 Обработка ноды {i+1}/{len(nodes_data)}: {custom_name} ({hardware_type})")
            
            peer_info = self.get_peer_info(node_id)
            if not peer_info:
                logger.warning(f"  ⚠️  Не удалось получить peer info для {node_id}")
                result = {
                    'id': node_id,
                    'custom_name': custom_name,
                    'api_name': 'UNKNOWN',
                    'hardware_type': hardware_type,
                    'address': eoa_addresses.get(node_id),
                    'reward': 0,  # Wins
                    'score': 0,   # Rewards
                    'online': False,
                    'last_tx_minutes_ago': None,
                    'timestamp': datetime.now().isoformat()
                }
                results.append(result)
                continue
            
            eoa_address = eoa_addresses.get(node_id)
            last_tx_minutes = None
            
            if eoa_address:
                last_tx_minutes = self.get_last_internal_tx_time(eoa_address)
            
            # Создаем результат с дополнительной информацией
            result = {
                'id': node_id,
                'custom_name': custom_name,
                'api_name': peer_info.get('peerName', 'Unknown'),
                'hardware_type': hardware_type,
                'address': eoa_address,
                'reward': peer_info.get('score', 0),      # score из API = Wins
                'score': peer_info.get('reward', 0),     # reward из API = Rewards
                'online': peer_info.get('online', False),
                'last_tx_minutes_ago': last_tx_minutes,
                'timestamp': datetime.now().isoformat()
            }
            
            results.append(result)
            
            status = "🟢"
            if last_tx_minutes is None:
                status = "⚫"
            elif last_tx_minutes > 30:
                status = "🔴"
            elif last_tx_minutes > 15:
                status = "🟡"
                
            logger.info(f"    {status} {custom_name} ({hardware_type}) | TX: {last_tx_minutes}м | Wins: {result['reward']} | Rewards: {result['score']}")
            
            time.sleep(1)
        
        logger.info(f"✅ Сбор данных завершен!")
        return results