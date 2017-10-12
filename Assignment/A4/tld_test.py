from tld import tld

def test_tld():
    assert tld("georgetown.edu") == "edu"
    assert tld("www.google.com") == "com"
    assert tld("nope") == "nope"

