# presto_benchmarking

Some scripts that help with benchmarking our experimental presto with additional
cudf features. We use these to keep track of what is running
at different scale factor and with different numbers of workers

bin/run_worker.sh allows a single presto worker to be run

bin/run_all_workers.sh allows eight workers to be run on eight different GPU
it assumes that there is a single workers directory containing all configurations

bin/tpch_sf{SIZE}_{NUM_WOKERS}.txt  records a list of queries seperated by newlines
that have run succesfully for a specific scale factor and a number of workers.

bin/test_tpch.sh allows a set of queries found in the tpch_sf*.txt file to be run
