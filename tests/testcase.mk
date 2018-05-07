# This file is included for every individual test
# Many of the variables here are set in Makefile in the comdb2/tests/ directory
# if they are not set it means that we are running make from within 
# the individual test directory thus we will need to define them


ifeq ($(TESTSROOTDIR),)
  # TESTSROOTDIR is not set when make is issued from within a test directory 
  # (will check assumption few lines later)
  # needs to expand to a full path, otherwise it propagates as '../'
  export TESTSROOTDIR=$(shell readlink -f $(PWD)/..)
#  export SKIPSSL=1   #force SKIPSSL for local test -- easier to debug
  export INSETUP=yes
else
  export INSETUP=
endif

# check that we indeed have the correct dir in TESTSROOTDIR
ifeq ($(wildcard ${TESTSROOTDIR}/setup),)
  $(error TESTSROOTDIR is set incorrectly to ${TESTSROOTDIR} )
endif

export SRCHOME?=$(shell readlink -f $(TESTSROOTDIR)/../)
ifeq ($(TESTID),)
export TESTID:=$(shell $(TESTSROOTDIR)/tools/get_random.sh)
endif
include $(TESTSROOTDIR)/Makefile.common


$(shell [ ! -f ${TESTDIR} ] &&  mkdir -p ${TESTDIR}/ )

export CURRDIR?=$(shell pwd)
export TESTCASE=$(patsubst %.test,%,$(shell basename $(CURRDIR)))
#comdb2 does not allow db names with '_' underscore in them
export DBNAME=$(subst _,,$(TESTCASE))$(TESTID)
export DBDIR=$(TESTDIR)/$(DBNAME)
export TMPDIR=$(TESTDIR)/tmp
export CDB2_CONFIG=$(abspath $(DBDIR)/comdb2db.cfg)
export CDB2_OPTIONS=--cdb2cfg $(CDB2_CONFIG)
export COMDB2_ROOT=$(TESTDIR)
export COMDB2_UNITTEST?=0

ifneq ($(INSETUP),)
  # we are in setup or running make from within a testdir
  $(shell TESTDIR="${TESTDIR}" CLUSTER="${CLUSTER}" SKIPSSL="${SKIPSSL}" ${TESTSROOTDIR}/tools/keygen.sh )
  $(shell TESTDIR="${TESTDIR}" CLUSTER="${CLUSTER}" TESTSROOTDIR="${TESTSROOTDIR}" COMDB2_EXE=${COMDB2_EXE} CDB2SQL_EXE=${CDB2SQL_EXE} COMDB2AR_EXE=${COMDB2AR_EXE} PMUX_EXE=${PMUX_EXE} pmux_port=${pmux_port} pmux_cmd=${pmux_cmd} ${TESTSROOTDIR}/tools/copy_files_to_cluster.sh > ${TESTDIR}/copy_files_to_cluster.log || echo "copy_files_to_cluster failed" )
endif

test:: tool unit
	echo "Working from dir `pwd`" >> $(TESTDIR)/test.log
	$(TESTSROOTDIR)/runtestcase
	$(MAKE) stop

clean::
	rm -f *.res

setup: tool
	@mkdir -p ${TESTDIR}/logs/
	@$(TESTSROOTDIR)/setup -debug
	@echo Ready to run

stop:

tool:

unit:
