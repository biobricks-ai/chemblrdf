#!/usr/bin/env bash

localpath=$(pwd)
echo "Local path: $localpath"

downloadpath="$localpath/download"
echo "Download path: $downloadpath"

temppath="$localpath/temp"
mkdir -p $temppath
echo "Temporal path: $temppath"

rawpath="$localpath/raw"
mkdir -p $rawpath
echo "Raw path: $rawpath"

cd $downloadpath
find . -type d > $temppath/dirs.txt
# find . -type f -name '*.ttl.gz' | cut -c 2- | cut -d"." -f2 | cut -f 2 -d '.' | sort > $temppath/files.txt
find . -type f -name '*.ttl.gz' | cut -c 2- | sed "s/.ttl.gz//" | sort > $temppath/files.txt

cd $rawpath
xargs mkdir -p < $temppath/dirs.txt
cd $rawpath

cat $temppath/files.txt | xargs -P14 -n1 bash -c '
if test -f '$rawpath'$1.ttl; then
  echo "unzip: file '$rawpath'$1.ttl already unzipped."
else
  gunzip -c -v '$downloadpath'$1.ttl.gz > '$rawpath'$1.ttl; 
fi' {}

