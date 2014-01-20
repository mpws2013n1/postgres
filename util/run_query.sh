#!/bin/bash

echo "$1" > q.sql
psql tpch -H -f q.sql -o r.html
chromium-browser r.html
