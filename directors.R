# functions
source(paste0(paths$current, "/main.R"))

# format json
director_files = fromJSON(paste0(paths$current, "/imdb/directing.json"))

# unlist json file
json_file <- lapply(director_files, function(x) {
  x[sapply(x, is.null)] <- NA
  unlist(x)
})

# bind columns to create data frame
df <- as.data.frame(do.call("cbind", json_file))

# from data frame to data table
directors = as.data.table(df)

# select tconst columns
train = as.data.table(dbGetQuery(con, "SELECT tconst FROM train"))

train = merge(train, directors, by.x = "tconst", by.y = "movie", all.x = T)
