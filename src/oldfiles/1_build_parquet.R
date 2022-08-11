pacman::p_load(rdflib, glue, tidyverse, pbapply, arrow)

ttl <- fs::dir_ls("download/ftp.ebi.ac.uk/pub/databases/RDF/chembl/latest",regex="ttl.gz$")

temp <- withr::local_tempdir()
pbapply::pblapply(ttl,function(file.gz){
  
  system(glue("gunzip {file.gz} -k -f "))
  file.ttl <- fs::path_ext_remove(file.gz)
  
    
  file.nquads <- fs::path_ext_set(file.ttl,"nquads")

  system(glue("rapper -i turtle -o nquads {file.ttl} > {file.nquads}"))

  lines <- readr::read_delim(file.nquads, delim=" ",
    col_names=c("subject","predicate","object","dot"),
    col_types=map(1:4,~ readr::col_character())) |> 
    select(-dot)

  path <- fs::path(temp,fs::path_file(file.ttl) |> fs::path_ext_set("parquet"))
  arrow::write_parquet(lines,path)
})

chembl <- open_dataset(temp)

path <- fs::path(temp,"chembl_30.0_activity.parquet")
activity <- arrow::read_parquet(path) |> tibble()

activity |> head() |> collect()
unique(activity$predicate)

substance <- arrow::read_parquet(fs::path(temp,"chembl_30.0_mol")) |> tibble()


# TESTING
file.ttl <- "download/ftp.ebi.ac.uk/pub/databases/RDF/chembl/latest/chembl_30.0_molecule.ttl"

head.material <- readr::read_lines(file.ttl,n_max=10000)
head.material <- head.material[1:which(head.material=="")[1]]
txt.connection <- file(file.ttl)
lines <- system(glue("wc -l {file.ttl}"),intern=T) |> strsplit(" ") |> pluck(1,1) |> as.integer()
readLines(txt.connection,n=length(head.material))

dir.ttl <- withr::local_tempdir()
chunk <- 1
residual <- c()
while(!isIncomplete(txt.connection)){
  cat(chunk,"\n"); chunk <- chunk+1e5;
  lines <- c(head.material,residual,readLines(txt.connection,n=1e5)) 
  last  <- which(lines=="") |> (\(v){ v[length(v)] })()
  residual <- lines[last:length(lines)]
  lines[1:blanklines[length(blanklines)]] |> writeLines(tempfile(tmpdir=dir.ttl,fileext=".ttl"))
}

dir.nquads <- withr::local_tempdir()
fs::dir_ls(dir.ttl)[1:100] |> purrr::walk(\(chunkfile){
  nquads <- tempfile(tmpdir=dir.nquads,fileext = "nquads")
  res <- system(glue("rapper -i turtle -o nquads {chunkfile} > {nquads}"),intern=T)
  if(!is.null(attr(res,"status"))){ stop("failed on ", chunkfile)}
})
