rm(list = ls());cat("\014")  # clear console and environment

# functions
paths <- list()
paths[["current"]] <- getwd()
paths[["data"]] <- paste0(paths$current, "/db/")
source(paste0(paths$current, "/main.R"))

ParseWriters <- function(paths, con){
  
  # format json
  writer_files = fromJSON(paste0(paths$current, "/imdb/writing.json"))
  
  # unlist json file
  json_file <- lapply(writer_files, function(x) {
    x[sapply(x, is.null)] <- NA
    unlist(x)
  })
  
  # bind columns to create data frame
  df <- as.data.frame(do.call("cbind", json_file))
  
  # from data frame to data table
  writers = as.data.table(df)
  
  # start duckdb connection
  duckdb = dbConnect(con$connection, dbdir = con$directory, read_only=FALSE)
  
  train = as.data.table(dbGetQuery(duckdb, "SELECT tconst, numVotes FROM train"))
  
  writers = merge(writers, train, by.x = "movie", by.y = "tconst", all.x = T)
  
  # TARGET ENCODING
  writers = TargetEncoding(writers, "writer", "numVotes")
  
  # set incremental index of writers to be used in dcast
  setDT(writers)[, Index := seq_len(.N), by = movie]
  
  # go from long to wide format
  writers = dcast(writers, movie ~ paste0("writer", Index), value.var = "value_mean")
  
  # select tconst columns
  train = merge(train[, .(tconst)], writers, by.x = "tconst", by.y = "movie", all.x = T)
  
  cat("writing to writers table")
  dbWriteTable(duckdb, "writers", train, overwrite = TRUE)
  
  dbDisconnect(duckdb, shutdown = T)
  
}

ParseWriters(paths, con)

