#!/bin/sh
 
 cd /home/fabian/mpws2013n1/pgsql
 make
 xterm -e "psql tpch"