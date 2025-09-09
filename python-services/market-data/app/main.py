from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
import threading
import json
import sys
import os
from datetime import datetime, timezone
import logging
from typing import Dict, Any, Optional
import signal

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

# Environment configuration
VERIFY_SSL = os.getenv('VERIFY_SSL', 'false').lower() == 'true'
from shared.kafka_client import KafkaClient
from websocket_service import Socket_conn_Binance

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('market_data_service.log')
    ]
)
logger = logging.getLogger(__name__)

kafka_client = KafkaClient()

class MarketDataCollector:
    def __init__(self):
        self.is_running = False
        self.websocket_thread: Optional[threading.Thread] = None
        self.websocket: Optional[Socket_conn_Binance] = None
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 5
        
        # Market data topics to subscribe to (must be lowercase)
        # Start with just one stream to test - using trade stream which is more reliable
        self.topics = [
            "btcusdt@trade"
        ]
        
    def _process_market_data(self, message: str) -> Optional[Dict[str, Any]]:
        """Process and validate incoming market data"""
        try:
            data = json.loads(message)
            logger.debug(f"Received raw data: {data}")
            
            # Handle different Binance stream formats
            if 'stream' in data and 'data' in data:
                # This is a stream wrapper format
                stream_data = data['data']
                stream_name = data['stream']
                logger.debug(f"Processing stream: {stream_name}")
            else:
                # Direct data format
                stream_data = data
                stream_name = "unknown"
            
            # Check if it's a trade stream
            if '@trade' in stream_name or 'trade' in stream_name:
                # Validate trade fields
                required_fields = ['s', 'p', 'q']
                if not all(key in stream_data for key in required_fields):
                    logger.warning(f"Invalid trade data structure: {stream_data}")
                    return None
                    
                processed_data = {
                    "symbol": stream_data["s"],
                    "price": float(stream_data["p"]),
                    "volume": float(stream_data["q"]),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "trade_id": stream_data.get("t"),
                    "is_buyer_maker": stream_data.get("m", False),
                    "source": "binance",
                    "stream_type": "trade",
                    "raw_data": stream_data
                }
                
                logger.info(f"Processed trade data for {processed_data['symbol']}: ${processed_data['price']}")
                return processed_data
            # Check if it's a ticker stream
            elif '@ticker' in stream_name or 'ticker' in stream_name:
                # Validate ticker fields
                required_fields = ['s', 'c', 'v', 'h', 'l', 'P']
                if not all(key in stream_data for key in required_fields):
                    logger.warning(f"Invalid ticker data structure: {stream_data}")
                    return None
                    
                processed_data = {
                    "symbol": stream_data["s"],
                    "price": float(stream_data["c"]),
                    "volume": float(stream_data["v"]),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "high_24h": float(stream_data["h"]),
                    "low_24h": float(stream_data["l"]),
                    "price_change_percent": float(stream_data["P"]),
                    "source": "binance",
                    "stream_type": "ticker",
                    "raw_data": stream_data
                }
                
                logger.info(f"Processed ticker data for {processed_data['symbol']}: ${processed_data['price']}")
                return processed_data
            else:
                logger.debug(f"Unhandled stream type: {stream_name}")
                return None
            
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.error(f"Error processing market data: {e}")
            logger.error(f"Raw message: {message}")
            return None
    
    def _on_message_handler(self, ws, message: str):
        """Handle incoming WebSocket messages"""
        try:
            processed_data = self._process_market_data(message)
            if processed_data:
                # Send to Kafka
                try:
                    kafka_client.produce_message("market.data", processed_data)
                    logger.info(f"Sent to Kafka: {processed_data['symbol']} = ${processed_data['price']}")
                except Exception as kafka_error:
                    logger.error(f"Failed to send to Kafka: {kafka_error}")
            else:
                logger.debug("No processed data to send")
                
        except Exception as e:
            logger.error(f"Error in message handler: {e}")
            logger.error(f"Message: {message}")
    
    def _on_connect_handler(self, ws):
        """Handle WebSocket connection"""
        logger.info("Successfully connected to Binance WebSocket")
        self.reconnect_attempts = 0
    
    def _on_error_handler(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"WebSocket error: {error}")
    
    def _on_close_handler(self, ws, status, msg):
        """Handle WebSocket close"""
        logger.warning(f"WebSocket closed: {status} - {msg}")
        if self.is_running:
            self._attempt_reconnect()
    
    def _attempt_reconnect(self):
        """Attempt to reconnect with exponential backoff"""
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            logger.error("Max reconnection attempts reached. Stopping collector.")
            self.is_running = False
            return
            
        self.reconnect_attempts += 1
        delay = min(self.reconnect_delay * (2 ** (self.reconnect_attempts - 1)), 60)
        
        logger.info(f"Attempting to reconnect in {delay} seconds (attempt {self.reconnect_attempts})")
        threading.Timer(delay, self._start_websocket).start()
    
    def _start_websocket(self):
        """Start WebSocket connection in a separate thread"""
        try:
            self.websocket = Socket_conn_Binance(
                topics=self.topics,
                futures=False,  # Using spot market
                on_message_handler=self._on_message_handler,
                on_connect=self._on_connect_handler,
                verify_ssl=VERIFY_SSL  # Use environment variable for SSL verification
            )
            
            # Override error and close handlers
            self.websocket.on_error = self._on_error_handler
            self.websocket.on_close = self._on_close_handler
            
            logger.info("Starting WebSocket connection...")
            self.websocket.run_forever()
            
        except Exception as e:
            logger.error(f"Failed to start WebSocket: {e}")
            if self.is_running:
                self._attempt_reconnect()
    
    def start(self):
        """Start the market data collector"""
        if self.is_running:
            logger.warning("Collector is already running")
            return
            
        self.is_running = True
        self.websocket_thread = threading.Thread(target=self._start_websocket, daemon=True)
        self.websocket_thread.start()
        logger.info("Market Data Collector started")
    
    def stop(self):
        """Stop the market data collector"""
        if not self.is_running:
            return
            
        logger.info("Stopping Market Data Collector...")
        self.is_running = False
        
        if self.websocket:
            self.websocket.close()
            
        if self.websocket_thread and self.websocket_thread.is_alive():
            self.websocket_thread.join(timeout=5)
            
        logger.info("Market Data Collector stopped")

# Global collector instance
collector = MarketDataCollector()

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        collector.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan context manager"""
    # Startup
    setup_signal_handlers()
    collector.start()
    logger.info("Market Data Service started")
    
    yield
    
    # Shutdown
    collector.stop()
    logger.info("Market Data Service stopped")

app = FastAPI(
    title="Market Data Service",
    description="Real-time cryptocurrency market data collector from Binance",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy" if collector.is_running else "unhealthy",
        "service": "market-data",
        "websocket_connected": collector.is_running,
        "reconnect_attempts": collector.reconnect_attempts,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/status")
async def get_status():
    """Detailed status endpoint"""
    return {
        "websocket_status": "connected" if collector.is_running else "disconnected",
        "kafka_topics": ["market.data"],
        "subscribed_symbols": collector.topics,
        "reconnect_attempts": collector.reconnect_attempts,
        "max_reconnect_attempts": collector.max_reconnect_attempts,
        "uptime": "running" if collector.is_running else "stopped"
    }

@app.get("/metrics")
async def get_metrics():
    """Metrics endpoint for monitoring"""
    return {
        "service": "market-data",
        "status": "running" if collector.is_running else "stopped",
        "reconnect_attempts": collector.reconnect_attempts,
        "websocket_thread_alive": collector.websocket_thread.is_alive() if collector.websocket_thread else False,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.post("/test/kafka")
async def test_kafka_producer():
    """Test Kafka producer with sample market data"""
    try:
        test_data = {
            "symbol": "BTCUSDT",
            "price": 50000.0,
            "volume": 1000.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "high_24h": 51000.0,
            "low_24h": 49000.0,
            "price_change_percent": 2.5,
            "source": "test",
            "stream_type": "test"
        }
        
        kafka_client.produce_message("market.data", test_data)
        logger.info("Test message sent to Kafka")
        
        return {
            "status": "success",
            "message": "Test data sent to Kafka",
            "data": test_data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Kafka test failed: {e}")
        raise HTTPException(status_code=500, detail=f"Kafka test failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
