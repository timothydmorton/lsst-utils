CWD=`pwd`

. setup_stack.sh
setup lsst_distrib -t w_2017_17
setup_local meas_mosaic

cd $CWD
