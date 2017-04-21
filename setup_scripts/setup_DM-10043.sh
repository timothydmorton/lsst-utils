CWD=`pwd`

. setup_stack.sh
setup lsst_distrib -t w_2017_15

ticket_branch="tickets/DM-9862"
setup_local afw u/tmorton/DM-9862 # rebased on to w.2017.15 
setup_local meas_astrom $ticket_branch
setup_local meas_mosaic $ticket_branch

lauren_branch="u/lauren/py11anet"
setup_local obs_subaru $lauren_branch
setup_local pipe_analysis $lauren_branch

cd $CWD
