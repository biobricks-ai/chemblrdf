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
    splitpath="'$datapath'$1.parquet"
    mkdir -p $splitpath
    echo "build_parquet: Split path $splitpath"
    echo "build_parquet: File '$rawpath'$1.ttl with size $filesize to split"
    echo "build_parquet: Converting file to nquads in single line without prefix."
    cat '$rawpath'$1.ttl | grep -v @prefix | awk -v RS= '"'"'{gsub(/\;\n/,"; ",$0)}1'"'"'  > $splitpath$1.ttl.t
    echo "build_parquet: Creating prefix file."
    cat '$rawpath'$1.ttl | grep @prefix > $splitpath$1.prefix
    echo "build_parquet: Spliting file in chunks of 10000000 lines."
    split -l 10000000 -d $splitpath$1.ttl.t $splitpath$1. --verbose > '$temppath'/$1.files
    echo "build_parquet: Extracting names of files."
    sed -i -e '"'"'s/creating\|file//g'"'"' '$temppath'/$1.files 
    echo "build_parquet: Processing splited files."
    cat '$temppath'/$1.files | xargs -P1 -n1 bash -c '"'"'
      echo "build_parquet: Processing file $1."
      splitpath="${1%/*}"
      echo "build_parquet: splitpath $splitpath"
      bnn=$(basename $1)
      echo "build_parquet: basename with number $bnn"
      bn=$(echo $bnn | sed '"'"'s/\.[^.]*$//'"'"')
      echo "build_parquet: basename $bn"
      echo "build_parquet: Creating file with prefix."
      cat $splitpath/$bn.prefix $splitpath/$bnn > $splitpath/$bnn.t
      rapper -i turtle -o nquads $splitpath/$bnn.t > $splitpath/$bnn.nquads
      python src/nquads2parquet.py $splitpath/$bnn.nquads $splitpath/$bnn.parquet
      rm $splitpath/$bnn.nquads;
      rm $splitpath/$bnn.csv;
      rm $splitpath/$bnn.t;
      rm $splitpath/$bnn;
    '"'"' {}
    rm $splitpath$1.ttl.t
    rm $splitpath$1.prefix
  else
    rapper -i turtle -o nquads '$rawpath'$1.ttl > '$datapath'$1.nquads;
    python src/nquads2parquet.py '$datapath'$1.nquads '$datapath'$1.parquet
    rm '$datapath'$1.nquads;
    rm '$datapath'$1.csv;
  fi
fi' {}
