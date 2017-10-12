#
# Parameters
export ASSIGNMENT=2
export REQUIRED_FILES=q1.txt q2_python.py q2_mapper.py q2_reducer.py q4_run.py q4_mapper.py q4_reducer.py q5_mapper.py q5_reducer.py q5_combiner.py q5_mapper.py
export OPTIONAL_FILES=q2_results.txt q2_plot.png q2_plot.pdf q3_results.py q3_plot.png q3_plot.pdf q4_monthly.txt q4_plot.png q4_plot.pdf q5_top_10.txt


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
