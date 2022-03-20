# paths
paths <- list()
paths[["current"]] <- getwd()
paths[["data"]] <- paste0(paths$current, "/db/")

# libraries
install.packages
library("DBI")
library("data.table")
library("jsonlite")
library("mltools")

# connect database
con = dbConnect(duckdb::duckdb(), dbdir=paste0(paths$data, "db.duckdb"), read_only=FALSE)


res = dbGetQuery(con,
                 "SELECT * FROM train1 
                 UNION
                 SELECT * FROM train2
                 UNION
                 SELECT * FROM train3 
                 UNION
                 SELECT * FROM train4
                 UNION
                 SELECT * FROM train5 
                 UNION
                 SELECT * FROM train6
                 UNION
                 SELECT * FROM train7 
                 UNION
                 SELECT * FROM train8")


