import json
from pathlib import Path
from urllib.request import Request, urlopen

base = 'http://schema-registry:8081'
subject = 'stock-trades-value'


def call(method, path, data=None):
  request = Request(base + path, method=method,
                    data=json.dumps(data).encode() if data is not None else None,
                    headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'})
  with urlopen(request, timeout=15) as response:
    return json.load(response)


for subject in ['stock-trades-value', 'stock-trades-processed-value', 'stock-trades-dlt-value']:
    call('PUT', '/config/' + subject, {'compatibility': 'BACKWARD_TRANSITIVE'})
    result = call('POST', '/subjects/' + subject + '/versions', {
      'schemaType': 'AVRO', 'schema': Path('/schemas/StockTrade.avsc').read_text()})
    assert call('GET', '/config/' + subject)['compatibilityLevel'] == 'BACKWARD_TRANSITIVE'
    print(f'Registered {subject}: schema ID {result["id"]}; BACKWARD_TRANSITIVE')
