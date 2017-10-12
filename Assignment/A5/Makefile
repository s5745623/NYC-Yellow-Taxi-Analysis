#
# Parameters
export ASSIGNMENT=5
export REQUIRED_FILES=quazyilx.py quazyilx_run.py quazyilx.py  quazyilx_test.py quazyilx_run.txt\
                      fwiki.py fwiki_test.py fwiki_run.py fwiki_run.txt
export OPTIONAL_FILES=

#
#
# Figure out which python we can use
PYTHON3=$(shell which python35 || which python3.5 || which python34 || which python3.4 || echo python3)

check:
	@$(PYTHON3) -c "print('Python3 is operational');"
	@$(PYTHON3) validator.py --check

submit:
	$(PYTHON3) validator.py --zip --check

clean:
	/bin/rm -f *~

quazyilx_run.txt: quazyilx_test.py
	spark-submit --files=quazyilx.py quazyilx_run.py

fwiki_run.txt: fwiki_test.py
	spark-submit --files=fwiki.py fwiki_run.py

