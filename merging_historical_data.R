#!/usr/bin/Rscript

### Merging Historical Data ###
#
# This script merges the historical data from the instances that run the 
# UNHCR portal here: http://data.unhcr.org/
# 
# The script should be run only once. After that the `scraper.R` script 
# will collect data automatically and store it in the same database with the 
# same attributes.


# The historical CSV files were collected manually from
# http://data.unhcr.org/. Each collection was put in a
# folder with the same name as the instance for easy
# merning. Some portals didn't allow the download
# of CSV files.

# This function reads the files in those folders and 
# loads them into a single data.frame.
LoadHistorical <- function() {
    folder_list <- list.files("data/portal")
    for (i in 1:length(folder_list)) {
        folder_location <- paste("data/portal/", folder_list[i], sep = "")
        file_list <- list.files(folder_location)
        for (j in 1:length(file_list)) {
                file_location <- paste(folder_location, "/", file_list[j], sep = "")
                a <- read.csv(file_location, sep = ";")
                names(a) <- c('updated_at', 'value')
                a$module_type <- 'Total Population & Demogrpahy'
                if (folder_list[i] == 'southsudan') {
                    file_name <- as.character(gsub(".csv", "", file_list[j]))
                    file_name <- gsub(" Total", ": Total", file_name)
                    a$module_name <- file_name
                } 
                if (folder_list[i] == 'car') {
                    file_name <- as.character(gsub(".csv", "", file_list[j]))
                    a$module_name <- file_name
                }
                else { 
                    file_name <- as.character(gsub(".csv", "", file_list[j]))
                    a$module_name <- file_name
                }
                a$name <- as.character(folder_list[i])
                
                # For processing more than 1 file per folder.
                if (length(file_list) > 1) {
                    if (j == 1) z <- a
                    else z <- rbind(z, a)
                    next
                }
                if (i == 1) z <- a
                else z <- rbind(z, a)
        }
    }
    z
}

historical_data <- LoadHistorical()

MergeHistorical <- function() {
    library(sqldf)
    
    # Loading scraper-collected, fresh data.
    db <- dbConnect(SQLite(), dbname="scraperwiki.sqlite")
    fresh_data <- dbReadTable(db, "unhcr_real_time")
    dbDisconnect(db)
    
    # Combining datasets
    z <- merge(fresh_data, historical_data, 
               by = c('name', 'module_name', 'value', 
                      'module_type', 'value'), all = TRUE)
    for (i in 1:nrow(z)) {
        if (is.na(z$updated_at.y[i]) == FALSE) { z$updated_at.x[i] <- as.character(z$updated_at.y[i]) }
    }
    z$updated_at.y <- NULL
    colnames(z)[ncol(z)] <- 'updated_at'
    z
}

merged_data <- MergeHistorical()

writeTables <- function() { 
message('Storing data in a database.')
db <- dbConnect(SQLite(), dbname="scraperwiki.sqlite")

    dbWriteTable(db, "unhcr_real_time", merged_data, row.names = FALSE, overwrite = TRUE)

    # for testing purposes
    # dbListTables(db)
    # x <- dbReadTable(db, "unhcr_real_time")
    # y <- dbReadTable(db, "_scraper_metadata")
    
    dbDisconnect(db)
    message('Done!')
}

writeTables()