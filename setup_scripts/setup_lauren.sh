source setup_stack.sh
setup lsst_distrib -t w_2017_10
setup -v -j astrometry_net_data ps1_pv2_20150302 -t w_2017_10
cd ~/lsst/repositories/obs_subaru
git checkout u/lauren/working
setup -v -r . -j -t w_2017_10
scons opt=3 -j4
cd ~/lsst/repositories/pipe_analysis
git checkout u/tmorton/testing
setup -v -r . -j -t w_2017_10
scons opt=3 -j4
cd ~/lsst/repositories/meas_mosaic
setup -v -r . -j -t w_2017_10
scons opt=3 -j4
