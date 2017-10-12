#
# Parameters
export ASSIGNMENT=4
export REQUIRED_FILES=mtr_fix.py q1.py q1.txt q2.py q2.txt tld.py q3.txt q3_counts.txt analytics.py top1k_analytics.py top1k_analytics.txt
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
