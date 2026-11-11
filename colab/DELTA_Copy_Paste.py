# @title 1. Siapkan library (tanpa GPU)
import importlib.util
import subprocess
import sys

for module, package in [('requests', 'requests>=2.32,<3'),
                        ('websocket', 'websocket-client>=1.8,<2')]:
    if importlib.util.find_spec(module) is None:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', package])
import requests
import websocket
if not hasattr(websocket, 'WebSocketApp'):
    raise RuntimeError('Modul websocket tidak cocok. Hapus paket websocket yang berbeda, lalu install websocket-client.')
print('✅ Library siap. Lanjutkan cell bot dan izinkan Google Drive.')

"""Bybit BTCUSDT delta alerts. Import-safe; no network calls until main()."""
import json
import math
import os
import queue
import random
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from pathlib import Path

SYMBOL = "BTCUSDT"
INTERVAL_MINUTES = 5
BASELINE_CANDLES = 24
DELTA_MULTIPLIER = 2.0
CLOSE_GRACE_SECONDS = 5
MAX_HISTORY = 2016  # Up to seven days of 5m records; gaps are not fabricated.
HISTORY_PATH = "/content/drive/MyDrive/btc-delta-bot/bybit-BTCUSDT-5m.json"
WIB = timezone(timedelta(hours=7))


def stamp(ms):
    return datetime.fromtimestamp(ms / 1000, WIB).strftime("%d %b %Y %H:%M")


def make_alert(record, baseline):
    delta = record["delta_btc"]
    volume = record["buy_btc"] + record["sell_btc"]
    sign = "POSITIF" if delta > 0 else "NEGATIF"
    ratio = abs(delta) / baseline
    return (
        f"🚨 DELTA {sign} · {SYMBOL} · {INTERVAL_MINUTES}m\n"
        f"Periode: {stamp(record['start_ms'])}–"
        f"{datetime.fromtimestamp(record['end_ms']/1000, WIB).strftime('%H:%M')} WIB\n"
        f"Buy taker: {record['buy_btc']:,.4f} BTC\n"
        f"Sell taker: {record['sell_btc']:,.4f} BTC\n"
        f"Delta: {delta:+,.4f} BTC ({100*delta/volume:+.2f}% volume)\n"
        f"Avg |delta| {BASELINE_CANDLES} candle tercatat sebelumnya: {baseline:,.4f} BTC\n"
        f"Rasio: {ratio:.2f}× · Trigger: ≥{DELTA_MULTIPLIER:g}×\n"
        f"Data: koneksi terpantau tanpa gangguan terdeteksi"
    )


class HistoryStore:
    def __init__(self, path):
        self.path = Path(path)

    def load(self):
        if not self.path.exists():
            return []
        try:
            data = json.loads(self.path.read_text())
            if (data['version'], data['exchange'], data['symbol'], data['interval_minutes']) != (1, 'Bybit linear', SYMBOL, INTERVAL_MINUTES):
                raise ValueError("metadata histori berbeda")
            rows = data['candles']
            seen = set()
            now_ms = int(time.time()*1000)
            for r in rows:
                start = r['start_ms']
                assert isinstance(start, int) and start % (INTERVAL_MINUTES*60000) == 0
                assert start not in seen
                seen.add(start)
                assert r['end_ms'] == start + INTERVAL_MINUTES*60000
                assert r['end_ms'] <= now_ms
                assert type(r['complete']) is bool
                assert all(math.isfinite(r[k]) for k in ('buy_btc','sell_btc','delta_btc'))
                assert r['buy_btc'] >= 0 and r['sell_btc'] >= 0
                assert math.isclose(r['delta_btc'], r['buy_btc']-r['sell_btc'], abs_tol=1e-7, rel_tol=1e-10)
            return sorted(rows, key=lambda r:r['start_ms'])[-MAX_HISTORY:]
        except Exception:
            raise RuntimeError("Histori tidak valid/tidak terbaca. Bot dihentikan agar file lama tidak tertimpa. Periksa file histori di Drive.") from None

    def save(self, rows):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = dict(version=1, exchange='Bybit linear', symbol=SYMBOL,
                    interval_minutes=INTERVAL_MINUTES, saved_at_utc=datetime.now(timezone.utc).isoformat(),
                    candles=rows[-MAX_HISTORY:])
        tmp = self.path.with_suffix('.tmp')
        with tmp.open('w') as f:
            json.dump(data, f, ensure_ascii=False, allow_nan=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)


class DeltaEngine:
    """Single-consumer engine. Out-of-order trades accepted until bucket finalization."""
    def __init__(self, history=None, now_ms=None):
        self.width = INTERVAL_MINUTES * 60000
        self.grace = CLOSE_GRACE_SECONDS * 1000
        self.history = list(history or [])
        self.buckets = {}
        self.connected = False
        self.eligible_start = math.inf
        self.started_ms = int(time.time()*1000) if now_ms is None else now_ms
        self.sealed_through = self.floor(self.started_ms) - self.width
        self.seen = OrderedDict()
        self.duplicates = 0
        self.late = 0
        self.dirty = False

    def floor(self, ms):
        return ms // self.width * self.width

    def connection(self, alive, ms):
        if not alive:
            self.connected = False
            self.eligible_start = math.inf
            for b in self.buckets.values():
                b['complete'] = False
        else:
            self.connected = True
            # Conservatively discard the subscription-start candle, even if boundary-adjacent.
            self.eligible_start = self.floor(ms) + self.width

    def trade(self, t):
        trade_id = str(t['i'])
        if trade_id in self.seen:
            self.duplicates += 1
            return
        ms, price, size, side = int(t['T']), float(t['p']), float(t['v']), t['S']
        if side not in ('Buy','Sell') or not all(math.isfinite(x) and x > 0 for x in (price,size)):
            raise ValueError('Trade fields invalid')
        self.seen[trade_id] = None
        if len(self.seen) > 100000:
            self.seen.popitem(last=False)
        start = self.floor(ms)
        if start <= self.sealed_through:
            self.late += 1
            # Never contaminate a new candle. Retract old bucket from future baselines.
            for old in self.history:
                if old['start_ms'] == start and old['complete']:
                    old['complete'] = False
                    self.dirty = True
            return
        b = self.buckets.setdefault(start, dict(start_ms=start, end_ms=start+self.width,
             buy_btc=0.0, sell_btc=0.0, complete=self.connected and start >= self.eligible_start))
        b['buy_btc' if side=='Buy' else 'sell_btc'] += size

    def advance(self, now_ms):
        closed = []
        for start in sorted(self.buckets):
            if start+self.width+self.grace > now_ms:
                break
            r = self.buckets.pop(start)
            r['delta_btc'] = r['buy_btc']-r['sell_btc']
            prior = [x for x in self.history if x['complete'] and x['start_ms'] < start][-BASELINE_CANDLES:]
            baseline = math.fsum(abs(x['delta_btc']) for x in prior)/BASELINE_CANDLES if len(prior)==BASELINE_CANDLES else None
            alert = None
            if r['complete'] and baseline is not None and baseline > 0 and abs(r['delta_btc']) >= DELTA_MULTIPLIER*baseline:
                alert = make_alert(r, baseline)
            # Candle state is detached/committed before any slow file or network work.
            self.history.append(r)
            self.history = self.history[-MAX_HISTORY:]
            self.dirty = True
            closed.append((r, baseline, alert))
        self.sealed_through = max(self.sealed_through, self.floor(now_ms-self.grace)-self.width)
        return closed


class GitHubHeartbeat:
    """Publish only while the consumer AND Bybit trade flow are fresh."""
    def __init__(self, repo, token):
        import re
        if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo):
            raise ValueError('GH_MONITOR_REPO harus owner/nama-repo')
        self.repo, self.token = repo, token

    @staticmethod
    def healthy(bot, now):
        return (not bot.stop_event.is_set() and bot.subscribed.is_set()
                and bot.engine.connected and not bot.overflow.is_set()
                and now-bot.last_loop < 30 and now-bot.last_trade < 90)

    def publish(self, session):
        from uuid import uuid4
        url = f'https://api.github.com/repos/{self.repo}/actions/variables'
        headers = {'Authorization': f'Bearer {self.token}',
                   'Accept': 'application/vnd.github+json',
                   'X-GitHub-Api-Version': '2022-11-28'}
        body = {'name': 'DELTA_HEARTBEAT', 'value': str(uuid4())}
        r = session.patch(url+'/DELTA_HEARTBEAT', headers=headers, json=body, timeout=(3,10))
        if r.status_code == 404:
            r = session.post(url, headers=headers, json=body, timeout=(3,10))
        if r.status_code not in (201,204):
            raise RuntimeError(f'HTTP {r.status_code}')

    def run(self, bot):
        import requests
        session = requests.Session()
        last_attempt = float('-inf')
        failed = False
        try:
            while not bot.stop_event.wait(1):
                now = time.monotonic()
                if now-last_attempt < 60 or not self.healthy(bot, now):
                    continue
                last_attempt = now
                try:
                    self.publish(session)
                    print('💓 Heartbeat GitHub terkirim.' if not failed else '✅ Heartbeat GitHub pulih.')
                    failed = False
                except Exception as exc:
                    import re
                    detail = str(exc) if isinstance(exc, RuntimeError) and re.fullmatch(r'HTTP [1-5][0-9]{2}', str(exc)) else type(exc).__name__
                    print(f'⚠️ Heartbeat GitHub gagal ({detail}); cek repo/secret/izin/koneksi.')
                    failed = True
        finally:
            session.close()


class DeltaBot:
    def __init__(self, token, chat_id, history_path=HISTORY_PATH, monitor=None):
        self.token, self.chat_id = token, chat_id
        self.stop_event = threading.Event()
        self.inbox = queue.Queue(maxsize=100000)
        self.outbox = queue.Queue(maxsize=200)
        self.overflow = threading.Event()
        self.store = HistoryStore(history_path)
        self.engine = DeltaEngine(self.store.load())
        self.ws = None
        self.last_rx = time.monotonic()
        self.opened_at = self.last_rx
        self.ws_thread = None
        self.sender_thread = None
        self.connected = threading.Event()
        self.subscribed = threading.Event()
        self.save_failed = False
        self.session_generation = 0
        self.monitor = monitor
        self.last_loop = time.monotonic()
        self.last_trade = float("-inf")

    def emit(self, event):
        try:
            self.inbox.put_nowait(event)
        except queue.Full:
            self.overflow.set()

    def _network(self):
        import websocket
        backoff = 1
        while not self.stop_event.is_set():
            self.session_generation += 1
            generation = self.session_generation
            open_time = None
            def opened(ws):
                nonlocal open_time
                open_time = time.monotonic()
                self.last_rx = open_time
                self.opened_at = open_time
                self.connected.set()
                ws.send(json.dumps({'op':'subscribe','args':[f'publicTrade.{SYMBOL}']}))
                print('🟡 WebSocket tersambung; menunggu konfirmasi subscription…')
            def received(ws, message):
                self.last_rx = time.monotonic()
                try:
                    data = json.loads(message)
                    if data.get('op') == 'subscribe':
                        if data.get('success') is not True:
                            print('⚠️ Subscription ditolak; mencoba ulang.')
                            ws.close()
                        elif not self.subscribed.is_set():
                            self.subscribed.set()
                            self.emit(('connection', True, int(time.time()*1000), generation))
                        return
                    if data.get('topic') != f'publicTrade.{SYMBOL}':
                        return
                    if not self.subscribed.is_set():
                        self.subscribed.set()
                        self.emit(('connection', True, int(time.time()*1000), generation))
                    rows = data.get('data')
                    if not isinstance(rows,list):
                        raise ValueError('bad data')
                    self.emit(('trades', rows, int(time.time()*1000), generation))
                except Exception:
                    self.emit(('fault', None, int(time.time()*1000), generation))
                    print('⚠️ Paket tidak dapat diproses; candle ditandai tidak lengkap.')
            def error(ws, exc):
                print(f'⚠️ WebSocket error ({type(exc).__name__}); reconnect akan dicoba.')
                self.emit(('fault', None, int(time.time()*1000), generation))
            def closed(ws, code, msg):
                self.connected.clear()
                self.subscribed.clear()
                self.emit(('connection', False, int(time.time()*1000), generation))
            self.ws = websocket.WebSocketApp('wss://stream.bybit.com/v5/public/linear',
                  on_open=opened,on_message=received,on_error=error,on_close=closed)
            try:
                self.ws.run_forever(ping_interval=20,ping_timeout=10)
            except Exception as exc:
                print(f'⚠️ Koneksi berakhir ({type(exc).__name__}).')
            finally:
                self.connected.clear()
                self.subscribed.clear()
                self.emit(('connection', False, int(time.time()*1000), generation))
            if self.stop_event.is_set():
                break
            if open_time and time.monotonic()-open_time > 60:
                backoff = 1
            delay = min(backoff,60) + random.random()
            print(f'🔄 Reconnect dalam {delay:.1f} detik…')
            self.stop_event.wait(delay)
            backoff = min(backoff*2,60)

    def _heartbeat(self):
        # Bybit JSON heartbeat, separate from protocol-level websocket-client ping.
        last = 0
        while not self.stop_event.wait(1):
            if not self.connected.is_set():
                continue
            if not self.subscribed.is_set() and time.monotonic()-self.opened_at > 15:
                if self.ws:
                    self.ws.close()
                continue
            if time.monotonic()-self.last_rx > 45:
                self.emit(('fault',None,int(time.time()*1000),self.session_generation))
                if self.ws:
                    self.ws.close()
                continue
            if time.monotonic()-last >= 20:
                try:
                    self.ws.send(json.dumps({'op':'ping'}))
                    last = time.monotonic()
                except Exception:
                    if self.ws:
                        self.ws.close()

    def _telegram(self):
        import requests
        session = requests.Session()
        try:
            while not self.stop_event.is_set() or not self.outbox.empty():
                try:
                    message = self.outbox.get(timeout=0.5)
                except queue.Empty:
                    continue
                delivered = False
                for attempt in range(3):
                    if self.stop_event.is_set():
                        break
                    wait = 2**attempt
                    try:
                        response = session.post(f'https://api.telegram.org/bot{self.token}/sendMessage',
                            json={'chat_id':self.chat_id,'text':message},timeout=(3,10))
                        payload = response.json()
                        if response.status_code==200 and payload.get('ok') is True:
                            delivered = True
                            break
                        print(f'⚠️ Telegram menolak pesan (HTTP {response.status_code}).')
                        if response.status_code==429:
                            wait = min(max(float(payload.get('parameters',{}).get('retry_after',5)),1),60)
                        elif response.status_code in (400,401,403,404):
                            break
                    except Exception as exc:
                        # Do not print exception URLs: they can contain the bot token.
                        print(f'⚠️ Pengiriman Telegram gagal ({type(exc).__name__}).')
                    if attempt < 2:
                        self.stop_event.wait(wait)
                if not delivered:
                    print('❌ Alert tidak terkonfirmasi terkirim. Periksa token/izin/chat atau koneksi. Tidak ada retry tanpa batas.')
                self.outbox.task_done()
        finally:
            session.close()

    def _save(self):
        if not self.engine.dirty:
            return
        try:
            self.store.save(self.engine.history)
            self.engine.dirty = False
            if self.save_failed:
                print('✅ Penyimpanan histori pulih.')
            self.save_failed = False
        except Exception as exc:
            if not self.save_failed:
                print(f'❌ Histori belum tersimpan ke Drive ({type(exc).__name__}); bot akan mencoba lagi.')
            self.save_failed = True

    def run(self):
        count = sum(x['complete'] for x in self.engine.history)
        print(f'🟢 Bybit {SYMBOL} · {INTERVAL_MINUTES}m · baseline {BASELINE_CANDLES} · trigger {DELTA_MULTIPLIER:g}×')
        print(f'📂 Histori lengkap dimuat: {count}. Baseline memakai candle tercatat, boleh melintasi jeda sesi.')
        print('ℹ️ Candle awal setelah subscription/reconnect dilewati. Ctrl+C / STOP untuk berhenti.')
        threads = [threading.Thread(target=self._network,daemon=True),
                   threading.Thread(target=self._telegram,daemon=True),
                   threading.Thread(target=self._heartbeat,daemon=True)]
        if self.monitor:
            threads.append(threading.Thread(target=self.monitor.run,args=(self,),daemon=True))
        else:
            print("ℹ️ Pemantauan GitHub belum dikonfigurasi; bot delta tetap berjalan.")
        self.ws_thread,self.sender_thread = threads[:2]
        for t in threads:
            t.start()
        last_save = 0
        try:
            while not self.stop_event.is_set():
                self.last_loop = time.monotonic()
                if self.overflow.is_set():
                    self.overflow.clear()
                    self.engine.connection(False,int(time.time()*1000))
                    print('❌ Antrean data penuh; koneksi diulang dan candle tidak lengkap dilewati.')
                    if self.ws:
                        self.ws.close()
                try:
                    event = self.inbox.get(timeout=0.25)
                    kind,value,received_ms,generation = event
                    if kind=='connection':
                        self.engine.connection(value,received_ms)
                        if value:
                            print('✅ Subscription aktif; mulai mengumpulkan transaksi.')
                    elif kind=='fault':
                        self.engine.connection(False,received_ms)
                        if self.ws:
                            self.ws.close()
                    elif kind=='trades':
                        for trade in sorted(value,key=lambda t:int(t['T'])):
                            try:
                                self.engine.trade(trade)
                                if abs(time.time()*1000-int(trade["T"])) < 90000:
                                    self.last_trade = time.monotonic()
                            except Exception:
                                self.engine.connection(False,received_ms)
                                print('⚠️ Transaksi tidak valid; candle dilewati dan koneksi diulang.')
                                if self.ws:
                                    self.ws.close()
                                break
                    self.inbox.task_done()
                except queue.Empty:
                    pass
                # Drain queued packets before closing candles; don't finalize behind backlog.
                if self.inbox.empty():
                    for record,baseline,alert in self.engine.advance(int(time.time()*1000)):
                        valid = record['complete']
                        count = sum(x['complete'] for x in self.engine.history[:-1])
                        status = 'LENGKAP' if valid else 'PARSIAL/GANGGUAN — skip'
                        info = f'baseline {baseline:,.4f}' if baseline is not None else f'warm-up {min(count,BASELINE_CANDLES)}/{BASELINE_CANDLES}'
                        print(f"[{stamp(record['start_ms'])} WIB] Δ {record['delta_btc']:+,.4f} BTC | {status} | {info}")
                        self._save()
                        if alert:
                            try:
                                self.outbox.put_nowait(alert)
                                print('📨 Trigger terpenuhi; alert masuk antrean Telegram.')
                            except queue.Full:
                                print('❌ Antrean Telegram penuh; alert ini tidak terkirim.')
                if time.monotonic()-last_save > 10:
                    self._save()
                    last_save = time.monotonic()
        except KeyboardInterrupt:
            print('\n🛑 Menghentikan bot…')
        finally:
            self.stop_event.set()
            if self.ws:
                self.ws.close()
            self._save()  # Active candle intentionally not persisted as complete.
            for t in threads:
                t.join(timeout=15)
            print('🛑 Bot berhenti. Candle lengkap terakhir disimpan jika Drive tersedia.')

    def stop(self):
        self.stop_event.set()
        if self.ws:
            self.ws.close()


def main():
    from google.colab import drive, userdata
    drive.mount('/content/drive')
    def secret(names):
        for name in names:
            try:
                value = userdata.get(name)
                if value and str(value).strip():
                    return str(value).strip()
            except Exception:
                continue
        raise RuntimeError('Colab Secret belum tersedia/diizinkan: '+names[0])
    token = secret(['TELEGRAM_BOT_TOKEN','TELEGRAM_TOKEN'])
    chat = secret(['TELEGRAM_CHAT_ID'])
    global _DELTA_BOT
    old = globals().get('_DELTA_BOT')
    if old is not None:
        old.stop()
        if old.ws_thread:
            old.ws_thread.join(timeout=15)
        if old.ws_thread and old.ws_thread.is_alive():
            raise RuntimeError('Bot sebelumnya belum berhenti. Hentikan cell lama sebelum menjalankan ulang.')
    monitor = None
    try:
        monitor_token = userdata.get('GH_MONITOR_TOKEN')
        monitor_repo = userdata.get('GH_MONITOR_REPO')
        if monitor_token and monitor_repo:
            monitor = GitHubHeartbeat(str(monitor_repo).strip(), str(monitor_token).strip())
    except Exception:
        print('ℹ️ Secret GitHub belum lengkap/diizinkan.')
    _DELTA_BOT = DeltaBot(token,chat,monitor=monitor)
    _DELTA_BOT.run()


if __name__ == '__main__':
    main()
