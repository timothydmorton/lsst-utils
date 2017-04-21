CWD=`pwd`

source setup_stack.sh
setup lsst_distrib -t w_2017_11 
cd ~/lsst/repositories/obs_subaru
git checkout u/lauren/working
setup -v -r . -j
scons opt=3 -j4 
cd ~/lsst/repositories/meas_mosaic
setup -r . -t w_2017_11
scons opt=3 -j4
cd ~/lsst/repositories/pipe_analysis
git checkout u/lauren/working
setup -v -r . -j
scons opt=3 -j4

cd $CWD
