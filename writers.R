# functions
source(paste0(paths$current, "/main.R"))

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

# set incremental index of writers to be used in dcast
setDT(writers)[, Index := seq_len(.N), by = movie]

# go from long to wide format
writers = dcast(writers, movie ~ paste0("writer", Index), value.var = "writer")

# select tconst columns
train = as.data.table(dbGetQuery(con, "SELECT tconst FROM train"))

train = merge(train, writers, by.x = "tconst", by.y = "movie", all.x = T)
