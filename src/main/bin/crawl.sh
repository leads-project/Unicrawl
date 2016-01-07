#!/bin/bash
# a basic crawler in bash
# usage: crawl.sh urlfile.txt <num-procs>
URLS_FILE=$1
CRAWLERS=$2

mkdir -p data/pages

WGET_CMD="wget \
  --tries=1 \
  --dns-timeout=30 \
  --connect-timeout=1 \
  --read-timeout=1 \
  --timestamping \
  --directory-prefix=data/pages \
  --wait=0 \
  --random-wait \
  --no-verbose \
  --reject *.jpg --reject *.gif \
  --reject *.png --reject *.css \
  --reject *.pdf --reject *.bz2 \
  --reject *.gz  --reject *.zip \
  --reject *.mov --reject *.fla \
  --reject *.xml \
  --no-check-certificate"

#  --recursive \
#  --level=1 \

< $URLS_FILE xargs -P $CRAWLERS -I _URL_ sh -c 'echo "_URL_"; $WGET_CMD "_URL_"'
