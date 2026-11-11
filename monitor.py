"""Short GitHub Actions watchdog. No trade or account credentials used."""
import json
import os
import time
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.error import HTTPError

HEARTBEAT = 'DELTA_HEARTBEAT'
STATE = 'DELTA_MONITOR_STATE'


class API:
    def __init__(self, repo, token):
        import re
        if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo):
            raise ValueError('Repository must be owner/name')
        self.base = f'https://api.github.com/repos/{repo}/actions/variables'
        self.token = token

    def request(self, method, url, body=None, telegram=False):
        data = None if body is None else json.dumps(body).encode()
        headers = {'Content-Type': 'application/json', 'User-Agent': 'btc-delta-monitor'}
        if not telegram:
            headers.update(Authorization=f'Bearer {self.token}', Accept='application/vnd.github+json',
                           **{'X-GitHub-Api-Version': '2022-11-28'})
        try:
            with urlopen(Request(url, data=data, headers=headers, method=method), timeout=20) as r:
                raw = r.read()
                return json.loads(raw) if raw else {}
        except HTTPError as e:
            # Exception text/URLs may contain Telegram tokens. Never print them.
            raise RuntimeError(f'HTTP {e.code}') from None
        except Exception:
            raise RuntimeError('Network request failed') from None

    def get(self, name):
        try:
            return self.request('GET', f'{self.base}/{name}')
        except RuntimeError as e:
            if str(e) == 'HTTP 404':
                # 404 can also mean repository access denied: verify repo access.
                self.request('GET', self.base + '?per_page=1')
                return None
            raise

    def put(self, name, value):
        body = {'name': name, 'value': json.dumps(value, separators=(',', ':'))}
        if self.get(name) is None:
            self.request('POST', self.base, body)
        else:
            self.request('PATCH', f'{self.base}/{name}', body)

    def notify(self, token, chat, text):
        result = self.request('POST', f'https://api.telegram.org/bot{token}/sendMessage',
                              {'chat_id': chat, 'text': text}, telegram=True)
        if result.get('ok') is not True:
            raise RuntimeError('Telegram rejected notification')


def transition(heartbeat, state, now, stale_seconds=600):
    """Return next state + optional message. Never advance state before delivery."""
    if heartbeat is None:
        return state, None  # Not armed: first healthy heartbeat required.
    timestamp = datetime.fromisoformat(heartbeat['updated_at'].replace('Z', '+00:00')).timestamp()
    age = now - timestamp
    if age < -60:
        raise ValueError('GitHub heartbeat time is in the future')
    previous = state.get('status', 'new')
    if age > stale_seconds:
        if previous != 'down':
            return {'status': 'down'}, (
                '🔴 BTC DELTA — heartbeat terlambat\n'
                f'Tidak ada heartbeat sehat selama {int(max(age, 0)//60)} menit.\n'
                'Colab, aliran transaksi, atau pengiriman heartbeat mungkin terputus. '
                'Periksa Colab; monitor tidak dapat menyalakan runtime otomatis.')
    else:
        if previous == 'down':
            return {'status': 'up'}, '🟢 BTC DELTA PULIH\nHeartbeat sehat diterima kembali; transaksi sedang diproses.'
        if previous == 'new':
            return {'status': 'up'}, '✅ PEMANTAU BTC DELTA AKTIF\nHeartbeat Colab diterima. Alert jika heartbeat terlambat lebih dari 10 menit.'
    return state, None


def run(api, token, chat, now=None):
    heartbeat = api.get(HEARTBEAT)
    raw_state = api.get(STATE)
    state = json.loads(raw_state['value']) if raw_state else {}
    if not isinstance(state, dict) or state.get('status', 'new') not in ('new', 'up', 'down'):
        raise ValueError('Invalid monitor state; refusing to overwrite')
    new_state, message = transition(heartbeat, state, time.time() if now is None else now)
    if message:
        api.notify(token, chat, message)
        api.put(STATE, new_state)
        print('Notification delivered; state saved.')
    elif heartbeat is None:
        print('NOT ARMED: waiting for first healthy Colab heartbeat.')
    else:
        print(f"Monitor OK; state={state.get('status', 'new')}.")


def main():
    names = ('GITHUB_REPOSITORY', 'GH_MONITOR_TOKEN', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID')
    settings = {key: os.environ.get(key, '').strip() for key in names}
    missing = [key for key, value in settings.items() if not value]
    if missing:
        raise RuntimeError('Missing configuration: ' + ', '.join(missing))
    run(API(settings['GITHUB_REPOSITORY'], settings['GH_MONITOR_TOKEN']),
        settings['TELEGRAM_BOT_TOKEN'], settings['TELEGRAM_CHAT_ID'])


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # No traceback: network exception chains may include credentials.
        print('MONITOR ERROR:', str(exc) if isinstance(exc, (RuntimeError, ValueError)) else type(exc).__name__)
        raise SystemExit(1)
