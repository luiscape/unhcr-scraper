#!/usr/bin/Rscript
#### Scraper to extact data from UNHCR's API. ####
# 
# The API is still very experimental and works in limited ways. 
# For example, in every 'instance' the structure of the API 
# changes slightly depending on the content that is available
# at the time. Navigating the JSON output is being a challenge. 
#
# Here I am making a query every day and storing the results in
# a database using ScraperWiki's platform. After fetching the data
# each new collection is compared with an old collection. 
# Collections that are new are added to 
# the database. The old ones are simply kept.

#### Scraper to extact data from UNHCR's API. ####

# library(jsonlite)
library(rjson)
library(RCurl)
library(sqldf)

# Instance specific url. 
# url <- 'http://data.unhcr.org/api/population/get_regional.json?instance_id=car'

# Function for colleting the refugee numbers.
PopulationNumbers <- function() { 
    
    # Fetching URL.
    url <- 'http://data.unhcr.org/api/population/get_regional.json'
    data <- fromJSON(getURL(url))
    messageText <- paste('Fetching UNHCR data from ~', length(data), 'public instances.')
    message(messageText)
    
    # Iterating over each instance.
    for(i in 1:length(data)) {
        
        # The second element is NULL.
        if (is.null(unlist(data[[i]][1])) == TRUE) { next }
        
        # The mali and thai instances seem to be empty.
        if (length(data[[i]][[1]]$population) == 0) { next }
        
        # Fetching the instance name.
        instance_name <- data[[i]][[1]]$name
        
        # Fetching the population numbers, module name, 
        # updated at time and module type.
        for (j in 1:length(data[[i]][[1]]$population)) {
            
            # Adding the instance name as identifier.
            
            if (j == 1) { itName <- NULL ; itName <- instance_name }
            else itName[j] <- instance_name
            
            # Fetching the module names.
            if (j == 1) {
                itModuleName <- NULL
                itModuleName <- as.character(data[[i]][[1]]$population[[j]]$module_name[[1]])
            }
            else { 
                itModuleName[j] <- 
                    as.character(data[[i]][[1]]$population[[j]]$module_name[[1]])
            }
            
            # Fetching module type.
            if (j == 1) { 
                itModuleType <- NULL
                itModuleType <- data[[i]][[1]]$population[[j]]$module_type
            }
            else { 
                itModuleType[j] <- data[[i]][[1]]$population[[j]]$module_type 
            }
            
            # Fetching value.
            if (j == 1) { itValue <- NULL ; itValue <- data[[i]][[1]]$population[[j]]$value }
            else { itValue[j] <- data[[i]][[1]]$population[[j]]$value }
            
            # Fetching updated at.
            if (j == 1) { itUpdatedAt <- NULL ; itUpdatedAt <- data[[i]][[1]]$population[[j]]$updated_at }
            else { itUpdatedAt[j] <- data[[i]][[1]]$population[[j]]$updated_at }
            
            instanceCollector <- data.frame(itName, itModuleName, itModuleType, 
                                            itValue, itUpdatedAt)
            
            names(instanceCollector) <- c('name', 'module_name', 
                                          'module_type', 'value', 'updated_at')
        }
        
        # Combining all collections.
        if (i == 1) { 
            z <- instanceCollector
        }
        else { z <- rbind(z, instanceCollector) }
    }
    mess <- paste('Returning only the instances that had data:', length(unique(z$name)))
    message(mess)
    z
}

# Storing the fresh collection in a data.frame
unhcrRealTimePopNumbers <- PopulationNumbers()

message('Storing data in a db.')
db <- dbConnect(SQLite(), dbname="scraperwiki.sqlite")

writeTables <- function() { 
    if ("unhcr_real_time" %in% dbListTables(db) == FALSE) { 
        dbWriteTable(db, "unhcr_real_time", unhcrRealTimePopNumbers, row.names = FALSE, overwrite = TRUE)
    }
    else { 
        oldData <- dbReadTable(db, "unhcr_real_time")
        newData <- merge(unhcrRealTimePopNumbers, y, all = TRUE)
        dbWriteTable(db, "unhcr_real_time", newData, row.names = FALSE, overwrite = TRUE)
    }
    
    
    # Generating scrape metadata.
    scrape_time <- as.factor(Sys.time())
    id <- paste(ceiling(runif(1, 1, 100)), format(Sys.time(), "%Y"), sep = "_")
    new_data <- as.factor(identical(oldData, newData))
    scraperMetadata <- data.frame(scrape_time, id, new_data)
    
    if ("_scraper_metadata" %in% dbListTables(db) == FALSE) {
        dbWriteTable(db, "_scraper_metadata", scraperMetadata, row.names = FALSE, overwrite = TRUE)    
    }
    else { 
        dbWriteTable(db, "_scraper_metadata", scraperMetadata, row.names = FALSE, append = TRUE)  
    }
}

writeTables()

# for testing purposes
dbListTables(db)
x <- dbReadTable(db, "unhcr_real_time")
y <- dbReadTable(db, "_scraper_metadata")

dbDisconnect(db)
message('done')
