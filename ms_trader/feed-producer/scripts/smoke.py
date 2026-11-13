"""Opt-in local integration smoke. Requires running feed app and Compose services.
Creates uniquely tagged demo trades, retaining them as verification evidence.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request
import urllib.error
import uuid

BASE = os.environ.get('FEED_BASE_URL', 'http://localhost:8083')
ROOT = Path(os.environ.get('FEED_DIRECTORY', Path(__file__).resolve().parent.parent / 'var/feeds'))
HEADER = 'tradeDate,side,symbol,quantity,price,currency,counterparty\n'
TAG = 'feed-smoke-' + uuid.uuid4().hex

def http(path, data=None):
    req = urllib.request.Request(BASE + path, data=None if data is None else json.dumps(data).encode(),
                                 headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)

def wait_request(request_id):
    for _ in range(300):
        state = http('/api/feeds/requests/' + request_id)
        if state['status'] not in ('QUEUED', 'RUNNING'):
            assert state['status'] == 'FINISHED', state
            return [http('/api/feeds/executions/' + str(i)) for i in state['executionIds']]
        time.sleep(.1)
    raise AssertionError('Feed scan timed out')

def feed(name, body):
    path = ROOT / 'inbox' / name
    path.write_text(HEADER + body)
    path.with_name(name + '.ready').touch()
    states = wait_request(http('/api/feeds/run', {'fileName': name})['requestId'])
    digest = hashlib.sha256((HEADER + body).encode()).hexdigest()
    return digest, next(s for s in states if s['feedId'] == digest)

def mongo(digest):
    prefix = '^feed:' + digest + ':row:'
    js = 'JSON.stringify(db.getSiblingDB("trader").trades.aggregate([' + json.dumps({
        '$match': {'idempotencyKey': {'$regex': prefix}}}) + ',' + json.dumps({
        '$group': {'_id': '$publicationStatus', 'count': {'$sum': 1}}}) + ']).toArray())'
    output = subprocess.check_output(['docker', 'exec', 'stock-trading-mongodb-1',
                                     'mongosh', '--quiet', '--eval', js], text=True)
    return {row['_id']: row['count'] for row in json.loads(output)}

try:
    http('/api/feeds/run', {'fileName': '../outside.csv'})
    raise AssertionError('Traversal filename accepted')
except urllib.error.HTTPError as error:
    assert error.code == 400
# A scan must ignore a CSV that its upstream writer has not marked ready.
unready = ROOT / 'inbox' / (TAG + '-unready.csv')
unready.write_text(HEADER)
wait_request(http('/api/feeds/run', {'fileName': unready.name})['requestId'])
assert unready.exists(), 'Unready CSV was claimed'
unready.unlink()
# Hold the same session advisory lock from another connection.
lock = subprocess.Popen(['docker', 'exec', 'stock-trading-feed-postgres-1', 'psql',
                         '-U', os.environ.get('FEED_POSTGRES_USER', 'feed_batch'), '-d', 'feed_batch',
                         '-c', 'select pg_advisory_lock(70612201); select pg_sleep(3);'],
                        stdout=subprocess.DEVNULL)
try:
    time.sleep(.5)
    request_id = http('/api/feeds/run', {})['requestId']
    for _ in range(50):
        state = http('/api/feeds/requests/' + request_id)
        if state['status'] not in ('QUEUED', 'RUNNING'):
            break
        time.sleep(.1)
    assert state['status'] == 'BUSY', state
finally:
    assert lock.wait(timeout=10) == 0

valid = '2026-10-24,BUY,AAPL,1,12.50,USD,' + TAG + '\n'
invalid = '2026-10-24,SELL,MSFT,1,0,USD,' + TAG + '\n'
# This smoke uses FEED_MAX_BAD_RECORDS=1 and FEED_MAX_BAD_PERCENT=100.
digest, rejected = feed(TAG + '-reject.csv', valid + invalid * 2)
assert rejected['rejected'] == 'true', rejected
assert mongo(digest) == {}, 'Rejected feed persisted trades'
digest, accepted = feed(TAG + '-accept.csv', valid + invalid + valid)
assert accepted['status'] == 'COMPLETED' and accepted['validRows'] == 2, accepted
for _ in range(200):
    counts = mongo(digest)
    if counts == {'PUBLISHED': 2}:
        break
    time.sleep(.1)
assert counts == {'PUBLISHED': 2}, counts
_, duplicate = feed(TAG + '-duplicate.csv', valid + invalid + valid)
assert duplicate['executionId'] == accepted['executionId'], duplicate
assert mongo(digest) == {'PUBLISHED': 2}, 'Duplicate content created duplicate trades'
print(json.dumps({'rejectedExecution': rejected['executionId'], 'acceptedExecution': accepted['executionId'],
                  'acceptedTrades': 2, 'kafkaAcknowledged': 2, 'duplicateContent': 'deduplicated'}))
