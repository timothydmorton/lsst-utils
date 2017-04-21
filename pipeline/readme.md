# Pipeline

## Installation

First, setup the LSST stack so you have the right version of python accessible, then:

```
git clone https://github.com/timothydmorton/lsst-utils
cd lsst-utils/pipeline
python setup.py install --user
```

## Usage

Create a pipeline definition file, along the lines of the following `cosmos.yaml`:

```yaml
# The root directory of data repo
data_root: /datasets/hsc/repo

# rerun will be [private/]$user/$ticket/$field unless explicitly specified
#  (private/ will be added if on lsst-dev)
user: tmorton
ticket: DM-10043
field: cosmos
rerun: private/tmorton/DM-10043/cosmos

batch_type: slurm
cores_per_node: 48 # VC

# List of visits to use for each filter. This also defines the list of filters
# used for this processing run.
visit:
        HSC-G: 11690..11712:2^29324^29326^29336^29340^29350^29352
        HSC-R: 1202..1220:2^23692^23694^23704^23706^23716^23718
        HSC-I: 1228..1232:2^1236..1248:2^19658^19660^19662^19680^19682^19684^19694^19696^19698^19708^19710^19712^30482..30504:2
        HSC-Y: 274..302:2^306..334:2^342..370:2^1858..1862:2^1868..1882:2^11718..11742:2^22602..22608:2^22626..22632:2^22642..22648:2^22658..22664:2
        HSC-Z: 1166..1194:2^17900..17908:2^17926..17934:2^17944..17952:2^17962^28354..28402:2
        NB0921: 23038..23056:2^23594..23606:2^24298..24310:2^25810..25816:2

# CCD's to use
ccd: 0..103

# Location of reference skymap.  If provided, this will be copied into
# $rerundir/deepCoadd
skymap: /home/tmorton/rc-tests/skymaps/cosmos/skyMap.pickle

tract: 0
#patch: 4,4^4,5 # enter patch if desired

pipeline:
    - singleFrameDriver
    - makeDiscreteSkyMap
    - mosaic
    - coaddDriver
    - multiBandDriver

kwargs:
    singleFrameDriver:
        time: 1200 # sec
        nodes: 4
    makeDiscreteSkyMap:
        time: 300
    mosaic:
        time: 1200
        numCoresForRead: 48
        diagnostics: True
        diagDir: /home/tmorton/mosaicDiag/cosmos
    coaddDriver:
        time: 100
        nodes: 4
        config: "assembleCoadd.doApplyUberCal=False makeCoaddTempExp.doApplyUberCal=False"
    multiBandDriver:
        time: 4000
        nodes: 24
    all:
        clobber-versions: True

```

Then run

```
runPipeline.py cosmos.yaml
```
and hopefully it will run all the correct commands, respecting all the necessary dependenices.  
Please let me know if it doesn't, and what other functionality you might find useful.
If you want to see what commands will be run, you can run
```
runPipeline.py cosmos.yaml --test
```
