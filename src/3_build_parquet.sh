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
  filesize=$(ls -l '$rawpath'$1.ttl | awk '"'"'{print $5}'"'"')
  if [ "$filesize" -gt 15000000000 ]; then
    echo "build_parquet: File '$rawpath'$1.ttl with size $filesize to split"
    echo "build_parquet: Converting file to nquads in single line without prefix."
    cat '$rawpath'$1.ttl | grep -v @prefix | awk -v RS= '"'"'{gsub(/\;\n/,"; ",$0)}1'"'"'  > '$datapath'$1.ttl.t
    echo "build_parquet: Creating prefix file."
    cat '$rawpath'$1.ttl | grep @prefix > '$datapath'$1.prefix
    echo "build_parquet: Spliting file in chunks of 10000000 lines."
    split -l 10000000 -d '$datapath'$1.ttl.t '$datapath'$1. --verbose > '$temppath'/split_files.txt
    echo "build_parquet: Extracting names of files."
    sed -i -e '"'"'s/creating\|file//g'"'"' '$temppath'/split_files.txt 
    echo "build_parquet: Processing splited files."
    cat '$temppath'/split_files.txt | xargs -P1 -n1 bash -c '"'"'
      echo "build_parquet: Processing file $1."
      bnn=$(basename $1)
      bn=$(echo $bnn | sed '"'"'s/\.[^.]*$//'"'"')
      echo "build_parquet: Creating file with prefix."
      cat '$datapath'/$bn.prefix '$datapath'/$bnn > '$datapath'/$bnn.t
      rapper -i turtle -o nquads '$datapath'/$bnn.t > '$datapath'/$bnn.nquads
      python src/nquads2parquet.py '$datapath'/$bnn.nquads
      rm '$datapath'/$bnn.nquads;
      rm '$datapath'/$bnn.csv;
      rm '$datapath'/$bnn.t;
      rm '$datapath'/$bnn;
    '"'"' {}
    rm '$datapath'$1.ttl.t
    rm '$datapath'$1.prefix
  else
    rapper -i turtle -o nquads '$rawpath'$1.ttl > '$datapath'$1.nquads;
    python src/nquads2parquet.py '$datapath'$1.nquads
    rm '$datapath'$1.nquads;
    rm '$datapath'$1.csv;
  fi
fi' {}
