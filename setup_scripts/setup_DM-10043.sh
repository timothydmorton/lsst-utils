CWD=`pwd`

. setup_stack.sh
setup lsst_distrib -t w_2017_16

setup_local afw
setup_local meas_base
setup_local meas_astrom
setup_local meas_mosaic
setup_local obs_subaru

# lauren_branch="u/lauren/py11anet"
# setup_local pipe_analysis $lauren_branch

cd $CWD
