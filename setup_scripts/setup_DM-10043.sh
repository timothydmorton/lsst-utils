CWD=`pwd`

export LSST_STACK_DIR=/software/lsstsw/stack
. setup_stack.sh
setup lsst_distrib -t current
setup_local meas_mosaic

cd $CWD
