class Config:
    TELEGRAM_BOT_TOKEN = ""
    TELEGRAM_CHAT_ID = ""
    MONITORING_INTERVAL_MINUTES = 30
    
    GENSYN_PEER_API_URL = "https://dashboard.gensyn.ai/api/v1/peer"
    GENSYN_RPC_URL = "https://gensyn-testnet.g.alchemy.com/public"
    GENSYN_CONTRACT_ADDRESS = "0xFaD7C5e93f28257429569B854151A1B8DCD404c2"
    GENSYN_CHAIN_ID = 685685
    
    # Конфигурация прокси
    # Установить None если прокси не нужен, или указать в формате "login:password@ip:port"
    PROXY = ""  # Пример: "mylogin:mypassword@192.168.1.100:8080"
    
    RESULTS_DIR = "results"
    HISTORY_DIR = "monitor_history"

    NODE_DATA_FILE = "data.xlsx"  
    LOG_FILE = "gensyn_monitor.log"