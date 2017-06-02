CWD=`pwd`
CWD=`pwd`
. setup_stack.sh
setup lsst_distrib -t current 

setup_local meas_algorithms tickets/DM-2891

cd $CWD
