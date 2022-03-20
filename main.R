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


res = dbGetQuery(con, "SELECT * FROM train")


