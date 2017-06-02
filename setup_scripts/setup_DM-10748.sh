CWD=`pwd`
if [ "$HOST" == "lsst-dev01.ncsa.illinois.edu" ]; then
    export LSST_STACK_DIR=/software/lsstsw/stack
fi

. setup_stack.sh
setup_local meas_mosaic tickets/DM-10737

cd $CWD
