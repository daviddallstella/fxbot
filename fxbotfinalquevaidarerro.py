# ==============================================================================
# 1. IMPORTS
# ==============================================================================

# --- Imports Padrão do Python ---
import os
import json
import time
import webbrowser
import requests
import datetime as dt
import logging
import faulthandler
from collections import deque
from threading import Lock, Event

# --- Imports de Bibliotecas de Terceiros (Data Science) ---
import pandas as pd
import numpy as np


# --- Imports do Twisted e da Biblioteca cTrader ---
from twisted.internet import reactor, threads
from ctrader_open_api.client import Client
from ctrader_open_api.tcpProtocol import TcpProtocol
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *

# --- Importações Corrigidas pelo Gerador ---
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOAOrderType, ProtoOATradeSide, ProtoOATrendbarPeriod
# -------------------------------------------


# ==============================================================================
# 2. CONFIGURAÇÃO GERAL E DA ESTRATÉGIA
# ==============================================================================
CLIENT_ID = "16341_wX2wUUuOKMoHnsot22LCWqpPVqxVCoD7RXWWSjSKJ2lucNAd9U"
CLIENT_SECRET = "af7YTmrrJoGivbEphcZHHtexgsc6LR9qVnzUjsZA4eLXAkf5Qz"
REDIRECT_URI = "http://localhost:8080"
TRADING_ACCOUNT_ID = 44123485
API_HOST = "demo.ctraderapi.com"
API_PORT = 5035
SYMBOL_MAPPING = {"AUDUSD": 5, "NZDUSD": 12}
REVERSE_SYMBOL_MAPPING = {v: k for k, v in SYMBOL_MAPPING.items()}

# Usar a enumeração importada diretamente
BUY = ProtoOATradeSide.BUY
SELL = ProtoOATradeSide.SELL
MARKET = ProtoOAOrderType.MARKET

# Parâmetros da Estratégia (AINDA A SEREM ATUALIZADOS NO PASSO 1.2)
PAIR_1 = 'AUDUSD'
PAIR_2 = 'NZDUSD'
ZSCORE_WINDOW = 10
ENTRY_THRESHOLD = 1.0
EXIT_THRESHOLD = 0.0
TRADE_VOLUME_IN_LOTS = 0.01
PIP_VALUE = 0.0001
HISTORICAL_BARS_TO_LOAD = 150

# ==============================================================================
# 3. FUNÇÕES E CLASSES DO BOT
# ==============================================================================

def get_access_token():
    token_file = "tokens.json"
    try:
        with open(token_file, "r") as f:
            tokens = json.load(f)
        logging.info(f"Tokens carregados de '{token_file}'")
        return tokens.get("access_token")
    except (FileNotFoundError, json.JSONDecodeError):
        logging.warning("Arquivo de tokens não encontrado ou inválido. Iniciando fluxo de autenticação.")
        auth_params = { "client_id": CLIENT_ID, "redirect_uri": REDIRECT_URI, "response_type": "code", "scope": "trading", "state": os.urandom(16).hex() }
        auth_url = requests.Request('GET', "https://connect.spotware.com/apps/auth", params=auth_params).prepare().url
        print(f"\n[AUTH] Por favor, abra esta URL no seu navegador e autorize a aplicação:\n{auth_url}")
        webbrowser.open(auth_url)
        auth_code = input("\n[AUTH] Cole o Código de Autorização aqui (parâmetro 'code' da URL de retorno): ")
        token_data = { "grant_type": "authorization_code", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "code": auth_code, "redirect_uri": REDIRECT_URI }
        response = requests.post("https://openapi.ctrader.com/apps/token", data=token_data)
        response.raise_for_status()
        token_response = response.json()
        with open(token_file, "w") as f:
            json.dump(token_response, f, indent=4)
        logging.info(f"Tokens obtidos e salvos com sucesso em '{token_file}'")
        return token_response.get("access_token")

class PositionManager:
    def __init__(self, account_id, client):
        self.account_id = account_id
        self.client = client
        self.volume_id = int(TRADE_VOLUME_IN_LOTS * 10000000)
        self.reset_position_state()
    def reset_position_state(self):
        self.position = 'FLAT'
        self.open_positions = {}
    def register_open_trade(self, position_id, symbol, trade_side, entry_price):
        if position_id not in self.open_positions:
            self.open_positions[position_id] = { "symbol": symbol, "trade_side": trade_side, "entry_price": entry_price }
            logging.info(f"[POS-MGR] Posição Aberta: ID {position_id} | {symbol} | Preço: {entry_price}")
    def register_close_trade(self, position_id):
        if position_id in self.open_positions:
            del self.open_positions[position_id]
        return not self.open_positions
    def execute_trade(self, signal):
        current_position = self.position
        if signal == 'GO_LONG' and current_position == 'FLAT':
            logging.info("Sinal de ENTRADA COMPRADA recebido. Abrindo posições...")
            self._send_market_order(PAIR_1, BUY)
            self._send_market_order(PAIR_2, SELL)
            self.position = 'LONG'
        elif signal == 'GO_SHORT' and current_position == 'FLAT':
            logging.info("Sinal de ENTRADA VENDIDA recebido. Abrindo posições...")
            self._send_market_order(PAIR_1, SELL)
            self._send_market_order(PAIR_2, BUY)
            self.position = 'SHORT'
        elif signal == 'CLOSE_POSITION' and current_position in ['LONG', 'SHORT']:
            logging.info("Sinal de FECHAMENTO recebido. Encerrando posições...")
            if current_position == 'LONG':
                self._send_market_order(PAIR_1, SELL)
                self._send_market_order(PAIR_2, BUY)
            elif current_position == 'SHORT':
                self._send_market_order(PAIR_1, BUY)
                self._send_market_order(PAIR_2, SELL)
            self.position = 'CLOSING'
    def _send_market_order(self, symbol_name, trade_side):
        request = ProtoOANewOrderReq()
        request.ctidTraderAccountId = self.account_id
        request.symbolId = SYMBOL_MAPPING[symbol_name]
        request.orderType = MARKET
        request.tradeSide = trade_side
        request.volume = self.volume_id
        request.comment = f"Robo ZScore {symbol_name}"
        request.label = f"zscore_bot_{int(time.time())}"
        self.client.send(request)
        trade_side_str = "COMPRA" if trade_side == BUY else "VENDA"
        logging.info(f"Ordem de {trade_side_str} enviada para {symbol_name}, {TRADE_VOLUME_IN_LOTS} lotes.")

class ZScoreStrategy:
    def __init__(self):
        self.prices = {PAIR_1: {'bid': None, 'ask': None}, PAIR_2: {'bid': None, 'ask': None}}
        self.bars_deque = deque(maxlen=ZSCORE_WINDOW * 5)
        self.z_score = None
        self.data_lock = Lock()

    def seed_historical_data(self, historical_data):
        with self.data_lock:
            logging.info(f"[STRATEGY] Semeando deque com {len(historical_data[PAIR_1])} barras históricas...")
            df1 = pd.DataFrame(historical_data[PAIR_1]).set_index("timestamp_ms")
            df2 = pd.DataFrame(historical_data[PAIR_2]).set_index("timestamp_ms")
            combined_df = pd.concat([df1.add_prefix(f"{PAIR_1}_"), df2.add_prefix(f"{PAIR_2}_")], axis=1).ffill().dropna()
            for timestamp, row in combined_df.iterrows():
                new_bar_dict = {
                    f'{PAIR_1}_mid': (row[f'{PAIR_1}_ask'] + row[f'{PAIR_1}_bid']) / 2,
                    f'{PAIR_2}_mid': (row[f'{PAIR_2}_ask'] + row[f'{PAIR_2}_bid']) / 2,
                    'timestamp': pd.Timestamp(timestamp, unit='ms', tz='UTC')
                }
                self.bars_deque.append(new_bar_dict)
        logging.info("[STRATEGY] Deque semeado. Calculando Z-Score inicial...")
        self.processar_nova_barra(is_historical=True)

    def on_tick(self, symbol_name, bid, ask):
        if symbol_name in self.prices:
            if bid is not None: self.prices[symbol_name]['bid'] = bid
            if ask is not None: self.prices[symbol_name]['ask'] = ask

    def processar_nova_barra(self, is_historical=False):
        p1_bid = self.prices[PAIR_1]['bid']; p1_ask = self.prices[PAIR_1]['ask']
        p2_bid = self.prices[PAIR_2]['bid']; p2_ask = self.prices[PAIR_2]['ask']
        if not is_historical:
            if None in [p1_bid, p1_ask, p2_bid, p2_ask]: return "[STRATEGY] Preços incompletos. Aguardando.", None
            mid_price1 = (p1_bid + p1_ask) / 2; mid_price2 = (p2_bid + p2_ask) / 2
            new_bar_dict = {f'{PAIR_1}_mid': mid_price1, f'{PAIR_2}_mid': mid_price2, 'timestamp': pd.Timestamp.now(tz='UTC')}
            self.bars_deque.append(new_bar_dict)
        with self.data_lock:
            current_bar_count = len(self.bars_deque)
            if current_bar_count < ZSCORE_WINDOW: return f"[STRATEGY] Coletando dados... {current_bar_count} de {ZSCORE_WINDOW} barras.", None
            bars_df = pd.DataFrame(list(self.bars_deque))
        bars_df.set_index('timestamp', inplace=True)
        bars_df['Ratio'] = bars_df[f'{PAIR_1}_mid'] / bars_df[f'{PAIR_2}_mid']
        bars_df['Ratio_MA'] = bars_df['Ratio'].rolling(window=ZSCORE_WINDOW).mean()
        bars_df['Ratio_STD'] = bars_df['Ratio'].rolling(window=ZSCORE_WINDOW).std()
        prev_z = self.z_score
        self.z_score = (bars_df['Ratio'].iloc[-1] - bars_df['Ratio_MA'].iloc[-1]) / bars_df['Ratio_STD'].iloc[-1]
        log_msg = f"[STRATEGY] Nova Barra. Z-Score: {self.z_score:.4f}"
        signal = None
        if prev_z is not None and not np.isnan(prev_z) and not np.isnan(self.z_score):
            if self.z_score > ENTRY_THRESHOLD and prev_z <= ENTRY_THRESHOLD: signal = 'GO_SHORT'
            if self.z_score < -ENTRY_THRESHOLD and prev_z >= -ENTRY_THRESHOLD: signal = 'GO_LONG'
            if (prev_z > EXIT_THRESHOLD and self.z_score <= EXIT_THRESHOLD) or \
               (prev_z < EXIT_THRESHOLD and self.z_score >= EXIT_THRESHOLD): signal = 'CLOSE_POSITION'
        return log_msg, signal

class CTraderHandler:
    def __init__(self, access_token):
        self.client = Client(API_HOST, API_PORT, TcpProtocol)
        self.access_token = access_token
        self.strategy = ZScoreStrategy()
        self.position_manager = PositionManager(TRADING_ACCOUNT_ID, self.client)
        self.last_bar_check_minute = -1
        self.last_bar_timestamp = None
        self._historical_data = {PAIR_1: [], PAIR_2: []}
        self._data_loaded_events = {symbol: Event() for symbol in SYMBOL_MAPPING.keys()}

    def start(self):
        self.client.setConnectedCallback(self._on_connected)
        self.client.setDisconnectedCallback(self._on_disconnected)
        self.client.setMessageReceivedCallback(self._on_message)
        self.client.startService()
        logging.info("Conectando ao servidor da cTrader...")

    def _on_connected(self, client):
        logging.info("Conectado. Enviando autenticação da aplicação...")
        request = ProtoOAApplicationAuthReq(clientId=CLIENT_ID, clientSecret=CLIENT_SECRET)
        client.send(request)

    def _on_disconnected(self, client, reason):
        logging.info(f"Desconectado: {reason}")
        if reactor.running: reactor.stop()

    def _request_historical_data(self):
        logging.info("[HIST] Requisitando dados históricos para 'cold start'...")
        to_timestamp = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
        from_timestamp = to_timestamp - (HISTORICAL_BARS_TO_LOAD * 60 * 60 * 1000)
        for symbol_name, symbol_id in SYMBOL_MAPPING.items():
            request = ProtoOAGetTrendbarsReq()
            request.ctidTraderAccountId = TRADING_ACCOUNT_ID
            request.period = ProtoOATrendbarPeriod.H1
            request.symbolId = symbol_id
            request.fromTimestamp = from_timestamp
            request.toTimestamp = to_timestamp
            self.client.send(request)
            logging.info(f"[HIST] Requisição enviada para {symbol_name}")

    def _on_all_data_loaded(self, _):
        logging.info("[HIST] Todos os dados históricos foram carregados.")
        self.strategy.seed_historical_data(self._historical_data)
        logging.info("Assinando feeds de preço em tempo real...")
        request = ProtoOASubscribeSpotsReq(ctidTraderAccountId=TRADING_ACCOUNT_ID, symbolId=list(SYMBOL_MAPPING.values()))
        self.client.send(request)

    def _on_message(self, client, msg):
        if msg.payloadType == ProtoOAApplicationAuthRes().payloadType:
            logging.info("Autenticação da aplicação bem-sucedida. Autenticando conta...")
            request = ProtoOAAccountAuthReq(ctidTraderAccountId=TRADING_ACCOUNT_ID, accessToken=self.access_token)
            client.send(request)

        elif msg.payloadType == ProtoOAAccountAuthRes().payloadType:
            logging.info(f"Autenticação da conta {TRADING_ACCOUNT_ID} bem-sucedida.")
            self._request_historical_data()

        elif msg.payloadType == ProtoOAGetTrendbarsRes().payloadType:
            response = ProtoOAGetTrendbarsRes()
            response.ParseFromString(msg.payload)
            symbol_name = REVERSE_SYMBOL_MAPPING.get(response.symbolId)
            
            if symbol_name:
                logging.info(f"[HIST] Recebidas {len(response.trendbar)} barras para {symbol_name}.")
                for bar in response.trendbar:
                    # ==============================================================================
                    # CORREÇÃO APLICADA AQUI
                    # Usando bar.Close (PascalCase) como o preço de referência da barra histórica,
                    # uma vez que 'bar.close' (lowercase) causava AttributeError.
                    # ==============================================================================
                    close_price = bar.Close / 100000.0
                    bar_data = {
                        "timestamp_ms": bar.utcTimestampInMinutes * 60 * 1000,
                        "bid": close_price, # Usando o fechamento como proxy
                        "ask": close_price  # Usando o fechamento como proxy
                    }
                    self._historical_data[symbol_name].append(bar_data)
                
                self._data_loaded_events[symbol_name].set() 

                if all(event.is_set() for event in self._data_loaded_events.values()):
                    threads.deferToThread(self._on_all_data_loaded, None)
            
        elif msg.payloadType == ProtoOASubscribeSpotsRes().payloadType:
            logging.info("Assinatura de feeds de preço realizada com sucesso.")
            reactor.callLater(1, self._main_loop)
        
        elif msg.payloadType == ProtoOASpotEvent().payloadType:
            event = ProtoOASpotEvent(); event.ParseFromString(msg.payload)
            symbol_name = REVERSE_SYMBOL_MAPPING.get(event.symbolId)
            if symbol_name:
                bid = event.bid / 100000.0 if event.HasField("bid") else None; ask = event.ask / 100000.0 if event.HasField("ask") else None
                self.strategy.on_tick(symbol_name, bid, ask)

        elif msg.payloadType == ProtoOAExecutionEvent().payloadType:
            event = ProtoOAExecutionEvent(); event.ParseFromString(msg.payload)
            if event.order.orderStatus == 2: # ORDER_STATUS_FILLED
                symbol_id = event.position.tradeData.symbolId; symbol_name = REVERSE_SYMBOL_MAPPING.get(symbol_id)
                if not symbol_name: logging.error(f"Não foi possível encontrar o nome do símbolo para o ID: {symbol_id}"); return
                if self.position_manager.position in ['LONG', 'SHORT']:
                    pos_id = event.position.positionId; trade_side = event.position.tradeData.tradeSide; exec_price = event.order.executionPrice
                    self.position_manager.register_open_trade(pos_id, symbol_name, trade_side, exec_price)
                elif self.position_manager.position == 'CLOSING':
                    pos_id = event.position.positionId; logging.info(f"[RESULT] Perna Fechada: {symbol_name} | ID: {pos_id}")
                    if self.position_manager.register_close_trade(pos_id):
                        logging.info("[POS-MGR] Posição geral fechada. Resetando estado."); self.position_manager.reset_position_state()
            else:
                logging.info(f"Evento de Execução: {event.order.orderStatus} para ordem {event.order.orderId}")
        
        elif msg.payloadType == ProtoOAOrderErrorEvent().payloadType:
            event = ProtoOAOrderErrorEvent(); event.ParseFromString(msg.payload); logging.error(f"Erro na Ordem: {event.description} | Codigo: {event.errorCode}")

    def _monitor_open_position(self):
        if self.position_manager.position == 'FLAT' or not self.position_manager.open_positions: return
        total_pnl_pips = 0; log_parts = []
        for pos_id, trade_details in self.position_manager.open_positions.items():
            symbol, entry_price, trade_side = trade_details["symbol"], trade_details["entry_price"], trade_details["trade_side"]
            current_prices = self.strategy.prices.get(symbol)
            if not current_prices or current_prices['bid'] is None or current_prices['ask'] is None: logging.warning(f"[MONITOR] Preços para {symbol} indisponíveis."); return
            pnl_pips = 0
            if trade_side == BUY: pnl_pips = (current_prices['bid'] - entry_price) / PIP_VALUE
            elif trade_side == SELL: pnl_pips = (entry_price - current_prices['ask']) / PIP_VALUE
            total_pnl_pips += pnl_pips; log_parts.append(f"{symbol} P/L: {pnl_pips:+.1f} pips")
        logging.info(f"[MONITOR] Posição Aberta: {', '.join(log_parts)} | Total P/L: {total_pnl_pips:+.1f} pips")

    def _main_loop(self):
        now = dt.datetime.now(dt.timezone.utc)
        if now.minute != self.last_bar_check_minute:
            self.last_bar_check_minute = now.minute
            if self.position_manager.position != 'FLAT': self._monitor_open_position()
            if self.last_bar_timestamp is not None:
                d = threads.deferToThread(self.strategy.processar_nova_barra)
                d.addCallbacks(self._handle_strategy_result, self._handle_strategy_error)
            self.last_bar_timestamp = now
        reactor.callLater(1, self._main_loop)

    def _handle_strategy_result(self, result):
        message, signal = result
        if message: logging.info(message)
        if signal: self.position_manager.execute_trade(signal)

    def _handle_strategy_error(self, failure):
        logging.error("Erro CRÍTICO no processamento da barra.", exc_info=failure)


# ==============================================================================
# 5. INICIALIZAÇÃO DO ROBÔ
# ==============================================================================
if __name__ == "__main__":
    log_file = open('faulthandler_crash.log', 'w')
    faulthandler.enable(file=log_file, all_threads=True)
    log_formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'); log_formatter.converter = time.gmtime
    root_logger = logging.getLogger(); root_logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler("robot_debug.log", mode='w'); file_handler.setFormatter(log_formatter); root_logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler(); stream_handler.setFormatter(log_formatter); root_logger.addHandler(stream_handler)

    access_token = get_access_token()
    if access_token:
        robot_handler = CTraderHandler(access_token)
        robot_handler.start()
        logging.info("Robô iniciado. Pressione Ctrl+C para parar.")
        reactor.run()
    else:
        logging.error("Não foi possível obter o Access Token. O robô não pode iniciar.")