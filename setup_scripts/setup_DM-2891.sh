CWD=`pwd`
. setup_stack.sh
setup lsst_distrib -t w_2017_13

setup_local meas_algorithms tickets/DM-2891

cd $CWD
