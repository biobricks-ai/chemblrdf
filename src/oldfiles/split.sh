cat chembl_30.0_molecule.ttl | grep -v @prefix | sed ':a;N;$!ba;s/\;\n/\; /g' > chembl_30.0_molecule.ttl.t1
cat chembl_30.0_molecule.ttl | grep @prefix > chembl_30.0_molecule.ttl.prefix
split -l 40000000 -d chembl_30.0_molecule.ttl.t1 chembl_30.0_molecule.ttl. 

cat chembl_30.0_molecule.ttl.prefix chembl_30.0_molecule.ttl.00 > chembl_30.0_molecule.ttl.p.00
rapper -i turtle -o nquads chembl_30.0_molecule.ttl.p.00 > chembl_30.0_molecule.ttl.00.nquads

cat chembl_30.0_molecule.ttl.prefix chembl_30.0_molecule.ttl.01 > chembl_30.0_molecule.ttl.p.01
rapper -i turtle -o nquads chembl_30.0_molecule.ttl.p.01 > chembl_30.0_molecule.ttl.01.nquads



  filesize=$(ls -l '$rawpath'$1.ttl | awk '{print $5}')
  maxfilesize=15000000000
  if [ "$filesize" -gt "$maxfilesize" ]; then
    echo "Big file to split"
  else