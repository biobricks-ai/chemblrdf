#!/usr/bin/env bash

localpath=$(pwd)
echo "Local path: $localpath"

downloadpath="$localpath/download"
echo "Download path: $downloadpath"

temppath="$localpath/temp"
echo "Temporal path: $temppath"

rawpath="$localpath/raw"
echo "Raw path: $rawpath"

datapath="$localpath/data"
mkdir -p $datapath
echo "Data path: $datapath"

cd $datapath
xargs mkdir -p < $temppath/dirs.txt
cd $localpath

cat $temppath/files.txt | xargs -P1 -n1 bash -c '
if test -f '$datapath'$1.parquet; then
  echo "build_parquet: file '$datapath'$1.parquet already created."
else
  rapper -i turtle -o nquads '$rawpath'$1.ttl > '$datapath'$1.nquads;
  python src/nquads2parquet.py '$datapath'$1.nquads
  rm '$datapath'$1.nquads;
  rm '$datapath'$1.csv;
fi' {}


