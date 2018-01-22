
# Make sure you set up the right stack.
HOST=`hostname`
if [[ "$HOST" == "tiger-sumire.princeton.edu" || ("$HOST" == "tigressdata.princeton.edu") ]]; then
	module load rh/devtoolset/6
fi

if [[ "$HOST" == "tiger-sumire.princeton.edu" || ("$HOST" == "tigressdata.princeton.edu") ]]; then
    export LSST_STACK_DIR=/tigress/HSC/LSST/stack3_tiger_20171219
elif [ $1 == "py3" ]; then
    export LSST_STACK_DIR=/ssd/lsstsw/stack3
elif [ $1 == "cluster" ]; then
    export LSST_STACK_DIR=/software/lsstsw/stack
else
    export LSST_STACK_DIR=/ssd/lsstsw/stack
fi


source $LSST_STACK_DIR/loadLSST.bash
setup lsst_distrib

function goto_repo {
	repo_dir=$LSST_REPO_DIR/$1
	
	# Clone the repo if it is not already cloned locally
	if [ ! -d $repo_dir ]; then
		cd $LSST_REPO_DIR
		git clone git@github.com:lsst/$1
		cd $repo_dir
	# Else, update to latest master
	else
		cd $repo_dir
		git checkout master
		git pull
	fi
	
	# Switch to desired branch if provided 
	if [ ! -z "$2" ]; then 
		git checkout $2 
	fi	
}

function setup_local {
	cwd=`pwd`
    goto_repo $1 $2

	if [ $3 == "rebase" ]; then
		git rebase master
	fi
	
	setup -v -r . -j
	#git clean -dfx
	scons opt=3 -j8 
    cd $cwd
}
