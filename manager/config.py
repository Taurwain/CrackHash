import os


class Config:
    PORT = int(os.getenv('PORT', 5000))
    WORKER_URLS = [
        os.getenv('WORKER1_URL', 'http://worker1:5001'),
        os.getenv('WORKER2_URL', 'http://worker2:5002'),
        os.getenv('WORKER3_URL', 'http://worker3:5003')
    ]
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 300))
    ALPHABET = 'abcdefghijklmnopqrstuvwxyz0123456789'
