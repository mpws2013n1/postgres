#!/bin/sh

make distclean
./configure --prefix=$HOME/postgres --enable-depend --enable-cassert --enable-debug
make
sudo make install
