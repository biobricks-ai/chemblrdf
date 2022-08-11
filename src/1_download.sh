#!/usr/bin/env bash

# Download files

localpath=$(pwd)
echo "Local path: $localpath"

downloadpath="$localpath/download"
echo "Download path: $downloadpath"
mkdir -p "$downloadpath"
cd $downloadpath;
ftpbase="ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBL-RDF/latest/"
wget -r -A ttl.gz -nH --cut-dirs=5 -nc $ftpbase
echo "Download done."
