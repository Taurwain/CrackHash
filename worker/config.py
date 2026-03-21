import os


class Config:
    PORT = int(os.getenv('PORT', 5001))
    WORKER_ID = os.getenv('WORKER_ID', 'worker1')
    MANAGER_URL = os.getenv('MANAGER_URL', 'http://manager:5000')
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
