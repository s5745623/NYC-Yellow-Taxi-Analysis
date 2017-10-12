import pytest
from quazyilx import *
import datetime

LINE1 = '2015-12-10 08:40:10 fnard:-1 fnok:-1 cark:-1 gnuck:-1'
LINE2 = '2015-12-10 08:40:10 fnard:-1 fnok:-2 cark:-1 gnuck:-1'
LINE3 = '2015-12-10 08:40:10 fnard:-1 fnok:-1 cark:-3 gnuck:-1'
LINE4 = '2015-12-10 08:40:10 fnard:-1 fnok:-1 cark:-1 gnuck:-4'

def test_Quazyilx():
    q = Quazyilx(LINE1)
    assert q.datetime.isoformat()=='2015-12-10T08:40:10'
    assert q.fnard==-1
    assert q.fnok==-1
    assert q.cark==-1
    assert q.gnuck==-1
    assert Quazyilx(LINE2).fnok==-2
    assert Quazyilx(LINE3).cark==-3
    assert Quazyilx(LINE4).gnuck==-4


if __name__=="__main__":
    q = Quazyilx(LINE1)
    print(q)
    print(q.datetime)
    print(q.datetime.isoformat())
    
