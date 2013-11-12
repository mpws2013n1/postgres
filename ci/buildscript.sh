#!/bin/sh

bin/pg_ctl stop -l logfile -D Dataspace
cd pgsql
./configure --prefix=/var/lib/jenkins/jobs/mpws2013n1/workspace --enable-depend --enable-cassert --enable-debug
make
make
make install
cd ..
bin/pg_ctl start -l logfile -D Dataspace
sleep 3
cd pgsql
make installcheck-parallel
cd ..
bin/psql ds2 -f test.sql -o actual_query_output

diff --ignore-all-space --ignore-blank-lines --side-by-side --suppress-common-lines actual_query_output expected_query_output > diff_output

if [ -s diff_output ]; then
	echo 'FAILED: Query output is not as expected'
else
	echo 'SUCCESS: Query output is as expected'
fi

bin/pg_ctl stop -l logfile -D Dataspace
