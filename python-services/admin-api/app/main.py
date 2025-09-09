from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import sys
import os
import redis
from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType
import threading
import json
import asyncio
import logging
import signal
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import time

# Add path to shared modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from shared.kafka_client import KafkaClient
from shared.database import get_db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('admin_api_service.log')
    ]
)
logger = logging.getLogger(__name__)

# Initialize clients
kafka_client = KafkaClient()
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def create_kafka_topic_if_not_exists(topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
    """Create Kafka topic if it doesn't exist"""
    try:
        # Get admin client from kafka_client
        admin_client = kafka_client.get_admin_client()
        
        # Check if topic exists
        metadata = admin_client.list_topics(timeout=10)
        if topic_name in metadata.topics:
            logger.info(f"Topic '{topic_name}' already exists")
            return True
        
        # Create topic
        from confluent_kafka.admin import NewTopic
        topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        
        fs = admin_client.create_topics([topic])
        
        # Wait for topic creation
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logger.info(f"Topic '{topic}' created successfully")
                return True
            except Exception as e:
                logger.error(f"Failed to create topic '{topic}': {e}")
                return False
                
    except Exception as e:
        logger.error(f"Error creating topic '{topic_name}': {e}")
        return False

def check_kafka_topic_exists(topic_name: str) -> bool:
    """Check if Kafka topic exists"""
    try:
        admin_client = kafka_client.get_admin_client()
        metadata = admin_client.list_topics(timeout=10)
        return topic_name in metadata.topics
    except Exception as e:
        logger.error(f"Error checking topic existence: {e}")
        return False

# Global state
consumer_thread = None

class MarketDataConsumer:
    def __init__(self):
        self.consumer = None
        self.is_running = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 5
        self.last_message_time = None
        
    def _process_market_data(self, data: Dict[str, Any]) -> bool:
        """Process and validate market data"""
        try:
            # Validate required fields
            required_fields = ['symbol', 'price', 'timestamp']
            if not all(field in data for field in required_fields):
                logger.warning(f"Invalid market data structure: {data}")
                return False
                
            # Store in Redis with proper key structure
            symbol = data['symbol'].upper()
            redis_key = f"price:{symbol}"
            
            # Add processing timestamp
            data['processed_at'] = datetime.now(timezone.utc).isoformat()
            
            # Store with TTL
            redis_client.set(redis_key, json.dumps(data))
            redis_client.expire(redis_key, 300)  # 5 minutes TTL
            
            # Update last message time
            self.last_message_time = datetime.now(timezone.utc)
            
            logger.debug(f"Processed market data for {symbol}: ${data['price']}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing market data: {e}")
            return False
    
    def _attempt_reconnect(self):
        """Attempt to reconnect with exponential backoff"""
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            logger.error("Max reconnection attempts reached. Stopping consumer.")
            self.is_running = False
            return
            
        self.reconnect_attempts += 1
        delay = min(self.reconnect_delay * (2 ** (self.reconnect_attempts - 1)), 60)
        
        logger.info(f"Attempting to reconnect in {delay} seconds (attempt {self.reconnect_attempts})")
        time.sleep(delay)
        self._start_consumer()
    
    def _start_consumer(self):
        """Start the Kafka consumer"""
        try:
            # Check if topic exists, create if not
            if not check_kafka_topic_exists("market.data"):
                logger.info("Topic 'market.data' does not exist, creating it...")
                if not create_kafka_topic_if_not_exists("market.data"):
                    logger.error("Failed to create topic 'market.data'")
                    if self.is_running:
                        self._attempt_reconnect()
                    return
            
            self.consumer = kafka_client.get_consumer("admin-group", ["market.data"])
            logger.info("Kafka consumer started successfully")
            self.reconnect_attempts = 0
            
            while self.is_running:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached end of partition")
                        continue
                    elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        logger.error(f"Topic not found: {msg.error()}")
                        if self.is_running:
                            self._attempt_reconnect()
                        return
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        if self.is_running:
                            self._attempt_reconnect()
                        return
                
                try:
                    # Parse message
                    data = json.loads(msg.value().decode('utf-8'))
                    self._process_market_data(data)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            if self.is_running:
                self._attempt_reconnect()
        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed")
    
    def start(self):
        """Start the consumer"""
        if self.is_running:
            logger.warning("Consumer is already running")
            return
            
        self.is_running = True
        self._start_consumer()
    
    def stop(self):
        """Stop the consumer"""
        if not self.is_running:
            return
            
        logger.info("Stopping Kafka consumer...")
        self.is_running = False
        
        if self.consumer:
            self.consumer.close()
            
        logger.info("Kafka consumer stopped")

# Global consumer instance
market_data_consumer = MarketDataConsumer()

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        market_data_consumer.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def check_redis_connection() -> bool:
    """Check Redis connection health"""
    try:
        redis_client.ping()
        return True
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        return False

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan context manager"""
    # Startup
    setup_signal_handlers()
    
    # Check Redis connection
    if not check_redis_connection():
        logger.error("Failed to connect to Redis. Service may not work properly.")
    
    # Start consumer in a separate thread
    global consumer_thread
    consumer_thread = threading.Thread(target=market_data_consumer.start, daemon=True)
    consumer_thread.start()
    logger.info("Admin API Service started")
    
    yield
    
    # Shutdown
    market_data_consumer.stop()
    if consumer_thread and consumer_thread.is_alive():
        consumer_thread.join(timeout=5)
    logger.info("Admin API Service stopped")

app = FastAPI(
    title="Trading Platform Admin API",
    description="Admin interface for trading platform",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    redis_healthy = check_redis_connection()
    consumer_healthy = market_data_consumer.is_running
    
    status = "healthy" if redis_healthy and consumer_healthy else "unhealthy"
    
    return {
        "status": status,
        "service": "admin-api",
        "version": "1.0.0",
        "redis_connected": redis_healthy,
        "kafka_consumer_running": consumer_healthy,
        "last_message_time": market_data_consumer.last_message_time.isoformat() if market_data_consumer.last_message_time else None,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/status")
async def get_status():
    """Detailed status endpoint"""
    return {
        "service": "admin-api",
        "kafka_consumer_status": "running" if market_data_consumer.is_running else "stopped",
        "redis_connected": check_redis_connection(),
        "reconnect_attempts": market_data_consumer.reconnect_attempts,
        "last_message_time": market_data_consumer.last_message_time.isoformat() if market_data_consumer.last_message_time else None,
        "uptime": "running" if market_data_consumer.is_running else "stopped"
    }

@app.get("/kafka/test")
async def test_kafka():
    """Test Kafka connectivity"""
    try:
        test_message = {
            "message": "Hello from admin API",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "admin-api"
        }
        kafka_client.produce_message("test-topic", test_message)
        return {
            "status": "success", 
            "message": "Message sent to Kafka",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Kafka test failed: {e}")
        raise HTTPException(status_code=500, detail=f"Kafka test failed: {str(e)}")

@app.post("/kafka/test-market-data")
async def test_market_data_topic():
    """Test market.data topic with sample data"""
    try:
        test_market_data = {
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
        kafka_client.produce_message("market.data", test_market_data)
        logger.info("Test market data sent to Kafka")
        return {
            "status": "success",
            "message": "Test market data sent to market.data topic",
            "data": test_market_data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Market data test failed: {e}")
        raise HTTPException(status_code=500, detail=f"Market data test failed: {str(e)}")

@app.post("/kafka/topics/create")
async def create_market_data_topic():
    """Create the market.data topic if it doesn't exist"""
    try:
        if create_kafka_topic_if_not_exists("market.data"):
            return {
                "status": "success",
                "message": "Topic 'market.data' created successfully",
                "topic": "market.data",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to create topic")
    except Exception as e:
        logger.error(f"Topic creation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Topic creation failed: {str(e)}")

@app.get("/kafka/topics")
async def list_kafka_topics():
    """List all available Kafka topics"""
    try:
        admin_client = kafka_client.get_admin_client()
        metadata = admin_client.list_topics(timeout=10)
        topics = list(metadata.topics.keys())
        
        return {
            "topics": topics,
            "count": len(topics),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to list topics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list topics: {str(e)}")

@app.get("/kafka/status")
async def get_kafka_status():
    """Get Kafka connection status and topic information"""
    try:
        admin_client = kafka_client.get_admin_client()
        metadata = admin_client.list_topics(timeout=10)
        
        topics = list(metadata.topics.keys())
        market_data_exists = "market.data" in topics
        
        return {
            "kafka_connected": True,
            "topics": topics,
            "market_data_topic_exists": market_data_exists,
            "topic_count": len(topics),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Kafka status check failed: {e}")
        return {
            "kafka_connected": False,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@app.get("/market-data/latest/{symbol}")
async def get_latest_price(symbol: str):
    """Get latest price for a specific symbol"""
    if not symbol or not symbol.strip():
        raise HTTPException(status_code=400, detail="Symbol cannot be empty")
    
    symbol = symbol.upper().strip()
    redis_key = f"price:{symbol}"
    
    try:
        data = redis_client.get(redis_key)
        if data:
            market_data = json.loads(data)
            return {
                "symbol": symbol,
                "data": market_data,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            logger.warning(f"No data found in Redis for symbol {symbol}")
            raise HTTPException(status_code=404, detail=f"No data found for symbol {symbol}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode Redis data for {symbol}: {e}")
        logger.error(f"Raw data: {data}")
        raise HTTPException(status_code=500, detail="Data corruption error")
    except Exception as e:
        logger.error(f"Error retrieving data for {symbol}: {e}")
        logger.error(f"Redis key: {redis_key}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/market-data/symbols")
async def get_available_symbols():
    """Get list of available symbols with their latest data"""
    try:
        keys = redis_client.keys("price:*")
        symbols_data = []
        
        for key in keys:
            symbol = key.split(":")[1]
            data = redis_client.get(key)
            if data:
                try:
                    market_data = json.loads(data)
                    symbols_data.append({
                        "symbol": symbol,
                        "price": market_data.get("price"),
                        "timestamp": market_data.get("timestamp"),
                        "processed_at": market_data.get("processed_at")
                    })
                except json.JSONDecodeError:
                    logger.warning(f"Failed to decode data for symbol {symbol}")
                    continue
        
        return {
            "symbols": [item["symbol"] for item in symbols_data],
            "count": len(symbols_data),
            "data": symbols_data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error retrieving symbols: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/debug/redis/keys")
async def debug_redis_keys():
    """Debug endpoint to see what keys are in Redis"""
    try:
        keys = redis_client.keys("*")
        key_data = {}
        
        for key in keys:
            try:
                data = redis_client.get(key)
                if data:
                    key_data[key] = {
                        "raw_data": data,
                        "parsed": json.loads(data) if data.startswith('{') else "Not JSON"
                    }
            except Exception as e:
                key_data[key] = {"error": str(e)}
        
        return {
            "keys": keys,
            "key_count": len(keys),
            "key_data": key_data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Debug Redis keys failed: {e}")
        raise HTTPException(status_code=500, detail=f"Debug failed: {str(e)}")

@app.get("/market-data/summary")
async def get_market_summary():
    """Get market summary with all available symbols"""
    try:
        keys = redis_client.keys("price:*")
        if not keys:
            return {
                "message": "No market data available",
                "symbols": [],
                "count": 0,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        
        symbols_data = []
        total_volume = 0
        
        for key in keys:
            symbol = key.split(":")[1]
            data = redis_client.get(key)
            if data:
                try:
                    market_data = json.loads(data)
                    symbols_data.append(market_data)
                    total_volume += market_data.get("volume", 0)
                except json.JSONDecodeError:
                    continue
        
        return {
            "total_symbols": len(symbols_data),
            "total_volume": total_volume,
            "symbols": symbols_data,
            "last_updated": max([item.get("timestamp", "") for item in symbols_data]) if symbols_data else None,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error generating market summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
