#!/bin/bash

echo "$1" > q.sql
psql $2 -H -f q.sql -o r.html
chromium-browser r.html
