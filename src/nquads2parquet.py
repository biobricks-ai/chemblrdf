import pandas as pd
import sys
import csv
import os

FileName = sys.argv[1]
FileNameCSV = os.path.splitext(FileName)[0] + ".csv"
FileNameParquet = os.path.splitext(FileName)[0] + ".parquet"

print(f"nquads2parquet: Creating CSV file {FileNameCSV}")
File = open(FileName, 'r')
Data = File.read()
File.close()

Data = Data.replace(" <", "\t<")
Data = Data.replace(" \"", "\t\"")
Data = Data.replace(" _", "\t_")
Data = Data.replace(" .", "")
# TO DO replace "@* by a regular expression using re.sub()
Data = Data.replace("\"@en", "\"")
Data = Data.replace("\\\"", "_")
# TO DO replace "^^* by a regular expression re.sub()
Data = Data.replace("\"^^<http://www.w3.org/2001/XMLSchema#float>", "\"")
Data = Data.replace("\"^^<http://www.w3.org/2001/XMLSchema#int>", "\"")
Data = Data.replace("\"^^<http://www.w3.org/2001/XMLSchema#date>", "\"")
Data = Data.replace("\"^^<http://www.w3.org/2001/XMLSchema#string>", "\"")
Data = Data.replace("\"^^<http://www.w3.org/2001/XMLSchema#dateTime>", "\"")
Data = Data.replace("\"^^<http://www.w3.org/2001/XMLSchema#boolean>", "\"")
Data = Data.replace("\"^^<http://www.w3.org/2001/XMLSchema#double>", "\"")

# Data = Data.replace("\"\n", "\"\t\n")

CSVFile = open(FileNameCSV, "wt")
CSVFile.write(Data)
CSVFile.close()

print(f"nquads2parquet: Creating parquet file {FileNameParquet}")
df = pd.read_csv(FileNameCSV, names=['subject', 'predicate', 'object'], 
    delimiter='\t', engine='python', quoting=csv.QUOTE_ALL)
df.to_parquet(FileNameParquet)