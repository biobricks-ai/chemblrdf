#!/usr/bin/env bash

localpath=$(pwd)
echo "Local path: $localpath"

if java --version; then
  echo "Command succeeded: java installed"
else
  sudo apt install default-jre
fi

if $HOME/rdfpro/rdfpro -v; then
  echo "Command succeeded: rdfpro installed"
else
  cd $HOME;
  wget https://knowledgestore.fbk.eu/files/rdfpro/0.6/rdfpro-dist-0.6-bin.tar.gz;
  tar xzvf rdfpro-dist-0.6-bin.tar.gz;
fi

if rapper -v; then
  echo "Command succeeded: rapper/rapter installed"
else
  sudo apt install raptor2-utils
fi

if aws --version; then
    echo "Command succeeded: awscli installed"
else
  sudo apt install unzip;
  wget "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -O "awscliv2.zip";
  unzip -o awscliv2.zip;
  sudo ./aws/install;
  sudo apt-get update;
  sudo apt-get install awscli;
fi

if R --version; then
  echo "Command succeeded: R installed"
else
  sudo apt install r-base-core
fi

if dvc --version; then
  echo "Command succeeded: dvc installed"
else
  sudo apt update;
  sudo apt install snapd;
  sudo snap install dvc --classic;
fi
