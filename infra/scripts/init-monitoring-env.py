"""Create missing local passwords without printing or replacing existing secrets."""
import os
import secrets
from pathlib import Path

path = Path(__file__).resolve().parent.parent / '.env'
current = path.read_text() if path.exists() else ''
missing = [key for key in ('GRAFANA_ADMIN_PASSWORD', 'FEED_POSTGRES_PASSWORD')
           if not any(line.startswith(key + '=') for line in current.splitlines())]
if missing:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(descriptor, 'w') as output:
        if current and not current.endswith('\n'):
            output.write('\n')
        for key in missing:
            output.write(key + '=' + secrets.token_urlsafe(24) + '\n')
    os.chmod(path, 0o600)
print('Local passwords ready in ignored infra/.env; existing values preserved')
