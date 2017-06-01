CWD=`pwd`
export LSST_STACK_DIR=/software/lsstsw/stack

. setup_stack.sh
setup_local meas_mosaic tickets/DM-10737

cd $CWD
