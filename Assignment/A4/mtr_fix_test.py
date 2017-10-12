from mtr_fix import *

LINE0 = 'Start: Wed Dec 28 23:37:02 2016'
LINE1 = '  2.|-- 96.120.104.177             0.0%     1    8.7   8.7   8.7   8.7   0.0'
TS = "2016-12-28T23:37:02"
def test_parse_timestamp():
    assert parse_timestamp(LINE0)==TS

def test_MtrLine():
    current_timestamp = parse_timestamp(LINE0)
    m = MtrLine(current_timestamp,LINE1)
    assert m.timestamp==TS
    assert m.hop_number==2
    assert m.ipaddr=='96.120.104.177'
    assert m.hostname==''
    assert m.pctloss==0
    assert m.time==8.7
    

