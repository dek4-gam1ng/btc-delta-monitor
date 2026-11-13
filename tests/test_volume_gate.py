import ast
import copy
import io
import json
import queue
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import sys
import types
SOURCE_DIR = Path(__file__).resolve().parent.parent / 'colab'
if not SOURCE_DIR.exists():
    SOURCE_DIR = Path(__file__).resolve().parent
source = (SOURCE_DIR / 'DELTA_Copy_Paste.py').read_text()
core = source[source.index('"""Bybit BTCUSDT'):]
tree = ast.parse(core)
tree.body = [node for node in tree.body if not (isinstance(node, ast.If)
             and isinstance(node.test, ast.Compare)
             and isinstance(node.test.left, ast.Name)
             and node.test.left.id == '__name__')]
bot = types.ModuleType('delta_bot')
sys.modules['delta_bot'] = bot
exec(compile(tree, str(SOURCE_DIR / 'DELTA_Copy_Paste.py'), 'exec'), bot.__dict__)

START=1699999800000
# Must be exactly aligned to 5m boundaries.
START=START//300000*300000

def record():
    return dict(start_ms=START,end_ms=START+300000,buy_btc=700.0,sell_btc=300.0,delta_btc=400.0,complete=True)

def payload(volume='1000', **kwargs):
    data=dict(retCode=0,time=START+305000,result=dict(symbol='BTCUSDT',category='linear',list=[[str(START),'1','1','1','1',volume,'100000000']]))
    data.update(kwargs)
    return data

class FastStop:
    def __init__(self):self.stopped=False;self.delays=[]
    def is_set(self):return self.stopped
    def set(self):self.stopped=True
    def wait(self,delay):self.delays.append(delay);return self.stopped

class GateTests(unittest.TestCase):
    def test_match_and_no_mutation(self):
        r=record();saved=copy.deepcopy(r)
        result=bot.VolumeGate.compare(r,payload())
        self.assertTrue(result['volume_match']);self.assertEqual(result['volume_diff_pct'],0)
        self.assertEqual(r,saved)
    def test_boundary_one_percent(self):
        r=record();r.update(buy_btc=710,delta_btc=410)
        self.assertTrue(bot.VolumeGate.compare(r,payload())['volume_match'])
        r.update(buy_btc=710.0001,delta_btc=410.0001)
        self.assertFalse(bot.VolumeGate.compare(r,payload())['volume_match'])
    def test_over_and_under_count(self):
        for v in ('900','1100'):
            self.assertFalse(bot.VolumeGate.compare(record(),payload(v))['volume_match'])
    def test_wrong_market_and_symbol(self):
        for key,value in [('category','spot'),('symbol','BTCUSD')]:
            p=payload();p['result'][key]=value
            with self.assertRaises(bot.VolumeCheckError):bot.VolumeGate.compare(record(),p)
    def test_wrong_candle_never_uses_next_or_previous(self):
        p=payload();p['result']['list'][0][0]=str(START+300000)
        with self.assertRaises(bot.VolumeCheckError):bot.VolumeGate.compare(record(),p)
    def test_unclosed_and_missing_time(self):
        for t in (START+299999,0):
            with self.assertRaises(bot.VolumeCheckError):bot.VolumeGate.compare(record(),payload(time=t))
    def test_invalid_api_response(self):
        with self.assertRaises(bot.VolumeCheckError):bot.VolumeGate.compare(record(),payload(retCode=10006))
    def test_bad_volume_fails_closed(self):
        for v in ('0','NaN','Infinity','-1'):
            with self.assertRaises(bot.VolumeCheckError):bot.VolumeGate.compare(record(),payload(v))
    def test_partial_record_rejected(self):
        r=record();r['complete']=False
        with self.assertRaises(bot.VolumeCheckError):bot.VolumeGate.compare(r,payload())
    def test_exact_request_and_retry_lagging_volume(self):
        session=Mock();session.get.side_effect=[SimpleNamespace(status_code=200,json=lambda:payload('950')),SimpleNamespace(status_code=200,json=lambda:payload())]
        stop=FastStop();check=bot.VolumeGate().verify(session,record(),stop)
        self.assertTrue(check['volume_match']);self.assertEqual(session.get.call_count,2)
        self.assertEqual(session.get.call_args.kwargs['params'],dict(category='linear',symbol='BTCUSDT',interval='5',start=START,end=START+299999,limit=1))
        self.assertEqual(stop.delays,[3])
    def test_mismatch_exhausts_retries(self):
        session=Mock();session.get.return_value=SimpleNamespace(status_code=200,json=lambda:payload('900'))
        with self.assertRaisesRegex(bot.VolumeCheckError,'selisih volume'):bot.VolumeGate().verify(session,record(),FastStop())
        self.assertEqual(session.get.call_count,3)
    def test_network_failure_is_sanitized(self):
        session=Mock();session.get.side_effect=RuntimeError('credential should not leak')
        with self.assertRaises(bot.VolumeCheckError) as cm:bot.VolumeGate().verify(session,record(),FastStop())
        self.assertNotIn('credential',str(cm.exception))
    def test_denial_or_rate_limit_no_retries(self):
        for code in (403,429):
            session=Mock();session.get.return_value.status_code=code
            with self.assertRaisesRegex(bot.VolumeCheckError,str(code)):bot.VolumeGate().verify(session,record(),FastStop())
            self.assertEqual(session.get.call_count,1)
    def test_shutdown_cancels_before_http(self):
        stop=FastStop();stop.set();session=Mock()
        with self.assertRaises(bot.VolumeCheckError):bot.VolumeGate().verify(session,record(),stop)
        session.get.assert_not_called()
    def test_record_invalidated_during_http(self):
        r=record();session=Mock()
        def changed(*args,**kwargs):
            r['complete']=False
            return SimpleNamespace(status_code=200,json=lambda:payload())
        session.get.side_effect=changed
        with self.assertRaises(bot.VolumeCheckError):bot.VolumeGate().verify(session,r,FastStop())
    def test_display_compact_and_trigger_preserved(self):
        r=dict(record(),**bot.VolumeGate.compare(record(),payload()))
        text=bot.make_alert(r,100)
        self.assertIn('Delta: +400.0 BTC',text)
        self.assertIn('Volume bot: 1,000.0 BTC',text)
        self.assertIn('Volume Bybit: 1,000.0 BTC',text)
        self.assertIn('Selisih: 0.00% · Batas ≤1%',text)
        self.assertIn('Trigger ≥4×',text)
    def test_sender_blocks_mismatch_and_sends_matching_only(self):
        for official,expected in [('900',0),('1000',1)]:
            with tempfile.TemporaryDirectory() as d:
                b=bot.DeltaBot('fake-token','fake-chat',str(Path(d)/'history.json'))
                b.stop_event=FastStop()
                class FinishingQueue(queue.Queue):
                    def task_done(self):super().task_done();b.stop_event.set()
                b.outbox=FinishingQueue()
                b.outbox.put({'record':record(),'baseline':100})
                session=Mock()
                session.get.return_value=SimpleNamespace(status_code=200,json=lambda:payload(official))
                session.post.return_value=SimpleNamespace(status_code=200,json=lambda:{'ok':True})
                with patch.dict('sys.modules',{'requests':SimpleNamespace(Session=lambda:session)}),redirect_stdout(io.StringIO()):b._telegram()
                self.assertEqual(session.post.call_count,expected)
                if expected:self.assertIn('Volume Bybit:',session.post.call_args.kwargs['json']['text'])
    def test_notebook_and_single_cell_consistent(self):
        nb=json.loads((SOURCE_DIR / 'BTC_Delta_Colab.ipynb').read_text())
        code=(SOURCE_DIR / 'DELTA_Copy_Paste.py').read_text()
        ast.parse(code)
        for c in nb['cells']:
            if c['cell_type']=='code':ast.parse(''.join(c['source']))
        self.assertTrue(code.startswith('# @title Slewbew Geming'))
        self.assertIn('DELTA_MULTIPLIER = 4.0',code)
        run=next(''.join(c['source']) for c in nb['cells'] if c['cell_type']=='code' and 'class DeltaBot:' in ''.join(c['source']))
        self.assertEqual(code[code.index('"""Bybit BTCUSDT'):],run[run.index('"""Bybit BTCUSDT'):])

if __name__=='__main__':unittest.main()
