from flask import Flask, request, jsonify, render_template
import hashlib
import uuid
import threading
import time
import requests
from datetime import datetime
from config import Config
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config.from_object(Config)

requests_data = {}
word_generators = {}
sent_batches = defaultdict(set)
sent_batches_info = defaultdict(dict)


class WordGenerator:
    def __init__(self, alphabet, max_length):
        self.alphabet = alphabet
        self.max_length = max_length
        self.current = [0]
        self.total_combinations = self._calculate_total()
        self.lock = threading.Lock()
        self.next_batch_number = 1

    def _calculate_total(self):
        total = 0
        for length in range(1, self.max_length + 1):
            total += len(self.alphabet) ** length
        return total

    def advance_batch(self, batch_size):
        with self.lock:
            if not self.current:
                return None
            batch_number = self.next_batch_number
            self.next_batch_number += 1
            for _ in range(batch_size):
                if self.current:
                    self._increment()
            return batch_number

    def _increment(self):
        pos = len(self.current) - 1
        while pos >= 0:
            self.current[pos] += 1
            if self.current[pos] < len(self.alphabet):
                return
            self.current[pos] = 0
            pos -= 1
        if pos < 0:
            if len(self.current) < self.max_length:
                self.current = [0] * (len(self.current) + 1)
            else:
                self.current = None


def get_worker_url(worker_name):
    urls = {
        'worker1': Config.WORKER_URLS[0],
        'worker2': Config.WORKER_URLS[1],
        'worker3': Config.WORKER_URLS[2]
    }
    return urls.get(worker_name, '').rstrip('/')


def check_worker_batch_status(worker_url, request_id, batch_number):
    try:
        response = requests.get(
            f"{worker_url}/internal/api/worker/status",
            params={'requestId': request_id, 'batchNumber': batch_number},
            timeout=5
        )
        if response.status_code == 200:
            return response.json().get('status')
    except Exception:
        pass
    return 'not_found'


def request_result_resend(worker_url, request_id, batch_number):
    try:
        requests.post(
            f"{worker_url}/internal/api/worker/resend",
            json={'requestId': request_id, 'batchNumber': batch_number},
            timeout=5
        )
    except Exception:
        pass


def reassign_batch(request_id, batch_number):
    available_workers = ['worker1', 'worker2', 'worker3']
    if batch_number in sent_batches_info[request_id]:
        failed_worker = sent_batches_info[request_id][batch_number]['worker']
        available_workers = [w for w in available_workers if w != failed_worker]
    if not available_workers:
        available_workers = ['worker1', 'worker2', 'worker3']
    new_worker = available_workers[0]
    logger.info(f"Reassigning batch {batch_number} to {new_worker}")
    sent_batches_info[request_id][batch_number] = {
        'worker': new_worker,
        'sent_at': time.time(),
        'status': 'sent',
        'retries': sent_batches_info[request_id].get(batch_number, {}).get('retries', 0) + 1
    }
    threading.Thread(target=resend_batch, args=(request_id, new_worker, batch_number)).start()


def resend_batch(request_id, worker_name, batch_number):
    worker_url = get_worker_url(worker_name)
    task_url = f"{worker_url}/internal/api/worker/hash/crack/task"
    task_data = {
        'requestId': request_id,
        'targetHash': requests_data[request_id]['target_hash'],
        'batchNumber': batch_number,
        'batchSize': Config.BATCH_SIZE,
        'maxLength': requests_data[request_id]['max_length'],
        'alphabet': Config.ALPHABET
    }
    if send_with_ack(task_url, task_data, request_id, batch_number):
        logger.info(f"Batch {batch_number} reassigned to {worker_name}")


def send_with_ack(task_url, task_data, request_id, batch_number):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(task_url, json=task_data, timeout=5)
            if response.status_code == 200 and response.json().get('status') == 'accepted':
                return True
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
        if attempt < max_retries - 1:
            time.sleep(1)
    return False


def monitor_batches(request_id):
    logger.info(f"Started monitoring for request {request_id}")
    while request_id in requests_data and requests_data[request_id]['status'] == 'IN_PROGRESS':
        time.sleep(5)
        for batch_number, info in list(sent_batches_info[request_id].items()):
            if info['status'] == 'sent' and time.time() - info['sent_at'] > 30:
                logger.warning(
                    f"Batch {batch_number} sent to {info['worker']} {time.time() - info['sent_at']:.0f}s ago, no result")
                worker_url = get_worker_url(info['worker'])
                status = check_worker_batch_status(worker_url, request_id, batch_number)
                if status == 'completed':
                    logger.info(f"Batch {batch_number} completed but result lost, requesting resend")
                    request_result_resend(worker_url, request_id, batch_number)
                    info['status'] = 'waiting_resend'
                elif status == 'processing':
                    logger.info(f"Batch {batch_number} still processing")
                    info['sent_at'] = time.time()
                elif status in ('not_found', 'failed'):
                    logger.warning(f"Batch {batch_number} lost, reassigning")
                    reassign_batch(request_id, batch_number)


def feed_worker(request_id, worker_name):
    logger.info(f"Started feeding {worker_name} for request {request_id}")
    worker_url = get_worker_url(worker_name)
    task_url = f"{worker_url}/internal/api/worker/hash/crack/task"
    while True:
        if request_id not in word_generators:
            break
        generator = word_generators[request_id]
        batch_number = generator.advance_batch(Config.BATCH_SIZE)
        if not batch_number:
            logger.info(f"No more batches for {worker_name}")
            break
        if batch_number in sent_batches[request_id]:
            logger.error(f"CRITICAL: Batch {batch_number} already sent!")
            continue
        sent_batches[request_id].add(batch_number)
        task_data = {
            'requestId': request_id,
            'targetHash': requests_data[request_id]['target_hash'],
            'batchNumber': batch_number,
            'batchSize': Config.BATCH_SIZE,
            'maxLength': requests_data[request_id]['max_length'],
            'alphabet': Config.ALPHABET
        }
        logger.info(f"Sending batch {batch_number} to {worker_name}")
        if send_with_ack(task_url, task_data, request_id, batch_number):
            sent_batches_info[request_id][batch_number] = {
                'worker': worker_name,
                'sent_at': time.time(),
                'status': 'sent',
                'retries': 0
            }
            logger.info(f"Batch {batch_number} accepted by {worker_name}")
        else:
            sent_batches[request_id].remove(batch_number)
            logger.warning(f"Batch {batch_number} returned to pool (worker {worker_name} failed)")


@app.route('/')
def index():
    return render_template('index.html', requests=requests_data, workers=Config.WORKER_URLS)


@app.route('/api/hash/crack', methods=['POST'])
def crack_hash():
    data = request.get_json()
    if not data or 'hash' not in data or 'maxLength' not in data:
        return jsonify({'error': 'Invalid request'}), 400
    request_id = str(uuid.uuid4())
    generator = WordGenerator(Config.ALPHABET, data['maxLength'])
    word_generators[request_id] = generator
    total_batches = (generator.total_combinations + Config.BATCH_SIZE - 1) // Config.BATCH_SIZE
    requests_data[request_id] = {
        'status': 'IN_PROGRESS',
        'target_hash': data['hash'],
        'max_length': data['maxLength'],
        'words_found': [],
        'created_at': datetime.now(),
        'progress': 0,
        'total_batches': total_batches,
        'completed_batches': 0
    }
    logger.info(f"Created request {request_id} with {total_batches} batches")
    for i, worker_url in enumerate(Config.WORKER_URLS):
        worker_name = f"worker{i + 1}"
        threading.Thread(target=feed_worker, args=(request_id, worker_name)).start()
    threading.Thread(target=monitor_batches, args=(request_id,)).start()
    return jsonify({'requestId': request_id})


@app.route('/api/hash/status', methods=['GET'])
def get_status():
    request_id = request.args.get('requestId')
    if not request_id or request_id not in requests_data:
        return jsonify({'error': 'Request not found'}), 404
    req = requests_data[request_id]
    return jsonify({
        'status': req['status'],
        'data': req['words_found'] if req['status'] == 'READY' else None
    })


@app.route('/internal/api/manager/hash/crack/request', methods=['PATCH'])
def receive_result():
    data = request.get_json()
    request_id = data.get('requestId')
    worker_id = data.get('workerId')
    words_found = data.get('words', [])
    batch_number = data.get('batchNumber')
    status = data.get('status')
    logger.info(f"Received result from {worker_id} for request {request_id}, batch {batch_number}, status: {status}")
    if request_id not in requests_data:
        return jsonify({'error': 'Request not found'}), 404
    req = requests_data[request_id]
    if batch_number not in sent_batches[request_id]:
        logger.warning(f"Received result for unsent batch {batch_number}")
        return jsonify({'status': 'ignored'}), 200
    if request_id in sent_batches_info and batch_number in sent_batches_info[request_id]:
        if sent_batches_info[request_id][batch_number]['status'] == 'completed':
            logger.info(f"Duplicate result for batch {batch_number}, ignoring")
            return jsonify({'status': 'already_processed'}), 200
    if words_found:
        for word in words_found:
            computed_hash = hashlib.md5(word.encode()).hexdigest()
            if computed_hash == req['target_hash'] and word not in req['words_found']:
                req['words_found'].append(word)
                logger.info(f"Found word: {word}")
    req['completed_batches'] += 1
    req['progress'] = (req['completed_batches'] / req['total_batches']) * 100
    logger.info(f"Progress: {req['completed_batches']}/{req['total_batches']} batches ({req['progress']:.1f}%)")
    if request_id in sent_batches_info and batch_number in sent_batches_info[request_id]:
        sent_batches_info[request_id][batch_number]['status'] = 'completed'
        sent_batches_info[request_id][batch_number]['completed_at'] = time.time()
    if req['completed_batches'] >= req['total_batches']:
        req['status'] = 'READY'
        logger.info(f"Request {request_id} completed")
    return jsonify({'status': 'ok'})


@app.route('/debug/request/<request_id>', methods=['GET'])
def debug_request(request_id):
    if request_id not in requests_data:
        return jsonify({'error': 'Not found'}), 404
    return jsonify({
        'request': requests_data[request_id],
        'sent_batches': sorted(list(sent_batches.get(request_id, []))),
        'batches_info': sent_batches_info.get(request_id, {})
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=Config.PORT, debug=True)
