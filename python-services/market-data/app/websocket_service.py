import websocket
import json
import traceback
import time
import ssl


class Socket_conn_Binance(websocket.WebSocketApp):
    def __init__(self, url=None, topics=None, futures=False, on_message_handler=None, on_connect=None, verify_ssl=True):
        base_url = "wss://fstream.binance.com" if futures else "wss://stream.binance.com:9443"
        if url:
            base_url = url
        
        # Handle topics
        self.topics = None
        if isinstance(topics, str):
            self.topics = [topics]
        else:
            self.topics = topics
        
        # Construct URL with streams if topics are provided
        if self.topics:
            streams = "|".join(self.topics)
            websocket_url = f"{base_url}/stream?streams={streams}"
            print(f"Constructed WebSocket URL: {websocket_url}")
        else:
            websocket_url = f"{base_url}/stream"
            print(f"Constructed WebSocket URL (no streams): {websocket_url}")
        
        # Create SSL context based on verification setting
        if verify_ssl:
            # Production mode - verify certificates
            ssl_context = ssl.create_default_context()
        else:
            # Development mode - skip certificate verification
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        
        super().__init__(
            url=websocket_url,
            on_open=self.on_open,
            on_message=on_message_handler or self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_ping=self.on_ping,
            on_pong=self.on_pong
        )
        self.handler = on_message_handler
        self.on_connect = on_connect
        self.ssl_context = ssl_context
        self.verify_ssl = verify_ssl
        # self.run_forever()

    def on_open(self, ws):
        print(ws, 'Websocket was opened')
        if self.topics:
            self.send_subscribe(self.topics)
        if self.on_connect:
            self.on_connect(ws)

    def on_error(self, ws, error):
        print('on_error', ws, error)
        print(traceback.format_exc())

    def on_close(self, ws, status, msg):
        print('on_close', ws, status, msg)

    def on_message(self, ws, message):
        """Default message handler"""
        print('Received message:', message)

    def send_subscribe(self, topics):
        """Send subscription message for the given topics"""
        if not topics:
            return
        
        # For Binance WebSocket streams, the subscription is handled via URL parameters
        # The streams are already included in the URL during initialization
        print(f'Subscribed to topics via URL: {topics}')
        print(f'WebSocket URL: {self.url}')
    
    def on_ping(self, ws, message):
        """Handle ping messages"""
        print('Received ping:', message)
    
    def on_pong(self, ws, message):
        """Handle pong messages"""
        print('Received pong:', message)
    
    def run_forever(self, **kwargs):
        """Override run_forever to use our SSL context"""
        ssl_options = {"cert_reqs": ssl.CERT_NONE} if not self.verify_ssl else {}
        return super().run_forever(sslopt=ssl_options, **kwargs)