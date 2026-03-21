from flask import Flask, request, jsonify
import hashlib
import requests
from config import Config
import logging
import threading
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config.from_object(Config)

active_tasks = {}


class WordGenerator:
	def __init__(self, alphabet, max_length):
		self.alphabet = alphabet
		self.max_length = max_length

	def get_batch_by_offset(self, batch_number, batch_size):
		start_position = batch_number * batch_size
		words = []
		current = self._position_to_word(start_position)
		for _ in range(batch_size):
			if not current:
				break
			words.append(current)
			current = self._increment_word(current)
		return words

	def _position_to_word(self, position):
		if position < 0:
			return None
		length = 1
		while True:
			combinations = len(self.alphabet) ** length
			if position < combinations:
				break
			position -= combinations
			length += 1
			if length > self.max_length:
				return None
		word = []
		for _ in range(length):
			idx = position % len(self.alphabet)
			word.insert(0, self.alphabet[idx])
			position //= len(self.alphabet)
		return ''.join(word)

	def _increment_word(self, word):
		if not word:
			return None
		chars = list(word)
		pos = len(chars) - 1
		while pos >= 0:
			current_idx = self.alphabet.index(chars[pos])
			if current_idx < len(self.alphabet) - 1:
				chars[pos] = self.alphabet[current_idx + 1]
				return ''.join(chars)
			else:
				chars[pos] = self.alphabet[0]
				pos -= 1
		if len(chars) < self.max_length:
			return self.alphabet[0] * (len(chars) + 1)
		return None


def send_result_with_retry(result, task_key):
	manager_url = app.config['MANAGER_URL'].rstrip('/')
	callback_url = f"{manager_url}/internal/api/manager/hash/crack/request"
	max_retries = 5
	for attempt in range(max_retries):
		try:
			response = requests.patch(callback_url, json=result, timeout=10)
			if response.status_code == 200:
				logger.info(f"Result for batch {result['batchNumber']} accepted by manager")
				if task_key in active_tasks:
					del active_tasks[task_key]
				return
			else:
				logger.error(f"Manager rejected result, status: {response.status_code}")
		except Exception as e:
			logger.error(f"Attempt {attempt + 1} failed: {e}")
		if attempt < max_retries - 1:
			time.sleep(2 ** attempt)
	logger.error(f"Failed to send result for batch {result['batchNumber']} after {max_retries} attempts")


def process_task(task_data, task_key):
	request_id = task_data['requestId']
	target_hash = task_data['targetHash']
	batch_number = task_data['batchNumber']
	batch_size = task_data['batchSize']
	max_length = task_data['maxLength']
	alphabet = task_data['alphabet']
	logger.info(f"Worker {app.config['WORKER_ID']} processing batch {batch_number}")
	try:
		generator = WordGenerator(alphabet, max_length)
		words = generator.get_batch_by_offset(batch_number, batch_size)
		found_words = []
		for word in words:
			if hashlib.md5(word.encode()).hexdigest() == target_hash:
				found_words.append(word)
				logger.info(f"Worker {app.config['WORKER_ID']} found match: {word}")
		result = {
			'requestId': request_id,
			'workerId': app.config['WORKER_ID'],
			'batchNumber': batch_number,
			'words': found_words,
			'status': 'completed'
		}
		active_tasks[task_key]['result'] = result
		active_tasks[task_key]['status'] = 'completed'
		send_result_with_retry(result, task_key)
	except Exception as e:
		logger.error(f"Error processing batch {batch_number}: {e}")
		active_tasks[task_key]['status'] = 'failed'


@app.route('/health', methods=['GET'])
def health():
	return jsonify({'status': 'healthy', 'worker_id': app.config['WORKER_ID']})


@app.route('/internal/api/worker/hash/crack/task', methods=['POST'])
def receive_task():
	data = request.get_json()
	request_id = data['requestId']
	batch_number = data['batchNumber']
	task_key = (request_id, batch_number)
	logger.info(f"Worker {app.config['WORKER_ID']} received task for request {request_id}, batch {batch_number}")
	if task_key in active_tasks:
		logger.info(f"Batch {batch_number} already being processed, ignoring duplicate")
		return jsonify({'status': 'already_processing'}), 200
	active_tasks[task_key] = {
		'data': data,
		'status': 'processing',
		'started_at': datetime.now(),
		'result': None
	}
	threading.Thread(target=process_task, args=(data, task_key)).start()
	return jsonify({'status': 'accepted'})


@app.route('/internal/api/worker/status', methods=['GET'])
def task_status():
	request_id = request.args.get('requestId')
	batch_number = request.args.get('batchNumber', type=int)
	task_key = (request_id, batch_number)
	if task_key in active_tasks:
		return jsonify({'status': active_tasks[task_key]['status']})
	else:
		return jsonify({'status': 'not_found'}), 404


@app.route('/internal/api/worker/resend', methods=['POST'])
def resend_result():
	data = request.get_json()
	request_id = data['requestId']
	batch_number = data['batchNumber']
	task_key = (request_id, batch_number)
	if task_key in active_tasks and active_tasks[task_key]['status'] == 'completed':
		result = active_tasks[task_key]['result']
		threading.Thread(target=send_result_with_retry, args=(result, task_key)).start()
		return jsonify({'status': 'resending'})
	return jsonify({'status': 'not_found'}), 404


if __name__ == '__main__':
	logger.info(f"Starting worker {app.config['WORKER_ID']} on port {app.config['PORT']}")
	app.run(host='0.0.0.0', port=app.config['PORT'], debug=True)