library(tidyverse)
library(odbc) 
library(DBI)
library(sf)
library(leaflet)
library(leaflet.extras)
library(leaflet.providers)
library(SpatialKDE)
library(lubridate)
library(raster)
library(stars)
library(terra)
library(tidyterra)
library(digest)

# Suppress messages for prod
# oldw <- getOption("warn")
# options(warn = -1)
# options(message = FALSE)

#library(tidyr)

# Connect to the LPS Data Warehouse - set up a system DSN first called "DW"
con <- dbConnect(odbc::odbc(), "DW")

# Get LPS Occurrence Data
incident_data = dbGetQuery(con,"SELECT [Id],[OccurrenceFileNo],[UCRCode],[UCRSubcode],[UCR_Description],[StartTime],[XCoordinate]/100 XCoordinate,[YCoordinate]/100 YCoordinate,[latitude],[longitude]
FROM [RMS_dw_prod].[dbo].[C_VW_Occurrence_w_Location]
WHERE StartTime >= DATEADD(YEAR,-3, GETDATE())
AND LocationType like '%Occurrence%'
AND XCoordinate > 0
AND NOT (XCoordinate = 15522815	AND YCoordinate = 550514707)
",as.is = c(TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE))

category_data = dbGetQuery(con,"SELECT Code UCRCode, Subcode UCRSubcode, Category
FROM [RMS_dw_prod].[dbo].[C_TBL_UCR_Violations_Categories]
JOIN [dbo].[UCRViolation]
ON [C_TBL_UCR_Violations_Categories].Id = [UCRViolation].Id
WHERE Category NOT IN ('Admin','Court Offences','Traffic')")

incident_data = inner_join(incident_data,category_data, by = c("UCRCode","UCRSubcode")) %>% filter(Category != "Domestic")

# Get LPS Person Data (Suspects Only)
person_data = dbGetQuery(con,"SELECT [C_VW_Occurrence_w_Location].Id,[OccurrenceFileNo],CAST([LId] AS varchar) AS [LId],CAST([RId] AS varchar) AS [RId],[OccIvPerson].ClassificationG, Surname
,[XCoordinate]/100 XCoordinate,[YCoordinate]/100 YCoordinate,UCRCode,UCRSubcode,UCR_Description
FROM [RMS_dw_prod].[dbo].[C_VW_Occurrence_w_Location]
JOIN [dbo].[OccIvPerson]
ON [C_VW_Occurrence_w_Location].Id = [OccIvPerson].LId
JOIN [dbo].[Person]
ON [OccIvPerson].RId = [Person].Id
WHERE XCoordinate > 0
AND NOT (XCoordinate = 15522815	AND YCoordinate = 550514707) 
AND [C_VW_Occurrence_w_Location].StartTime >= DATEADD(YEAR,-3, GETDATE()) 
AND DeceasedTime is NULL 
AND (OccIvPerson.ClassificationG like '%Adult accused diverted%'
OR OccIvPerson.ClassificationG like '%Arrested%'
OR OccIvPerson.ClassificationG like '%Associated to search warrant%'
OR OccIvPerson.ClassificationG like '%Charged%'
OR OccIvPerson.ClassificationG like '%Charges recommended%'
OR OccIvPerson.ClassificationG like '%Escapee%'
OR OccIvPerson.ClassificationG like '%Firearms interest police candidate%'
OR OccIvPerson.ClassificationG like '%Firearms refused/revoked%'
OR OccIvPerson.ClassificationG like '%Observed%'
OR OccIvPerson.ClassificationG like '%Parole%'
OR OccIvPerson.ClassificationG like '%Person of interest%'
OR OccIvPerson.ClassificationG like '%Probation%'
OR OccIvPerson.ClassificationG like '%Special interest person%'
OR OccIvPerson.ClassificationG like '%Subject%'
OR OccIvPerson.ClassificationG like '%Suicidal%'
OR OccIvPerson.ClassificationG like '%Suspect%'
OR OccIvPerson.ClassificationG like '%Suspect chargeable%'
OR OccIvPerson.ClassificationG like '%Wanted%'
OR OccIvPerson.ClassificationG like '%Warned%'
OR OccIvPerson.ClassificationG like '%YP 12-17 criminal offence%'
OR OccIvPerson.ClassificationG like '%Youth accused diverted%')
",as.is = c(TRUE,TRUE,TRUE,TRUE,TRUE,TRUE))

person_data = inner_join(person_data,category_data, by = c("UCRCode","UCRSubcode"))

# Convert data frame to sf
incidents_sf = st_as_sf(incident_data, coords = c("XCoordinate", "YCoordinate"), crs = 3401)
person_data_sf = st_as_sf(person_data, coords = c("XCoordinate", "YCoordinate"), crs = 3401)

# Load the City Boundary (select WGS or projected)
city_boundary_sf = st_read("C:\\localdata\\CrimeMap\\City_Boundary.geojson") %>% st_set_crs(4326) %>% st_transform(crs=3401)
#sf_city_boundary = st_read("C:\\localdata\\CrimeMap\\City_Boundary.geojson") %>% st_set_crs(4326)

#incidents_sf_City <- st_intersection(incidents_sf, city_boundary_sf)

# create a City Vector Grid
grid_sf = create_grid_rectangular(city_boundary_sf, cell_size = 100)

# create a City Raster Grid
raster_grid = create_raster(city_boundary_sf, cell_size = 100)

# Set the time window (30 days)
time_window <- 30

# Save the HS for saving...
hot_spot_list = list()
hslno = 0

# Iterate over unique crime types
crime_types <- sort(unique(incidents_sf$Category))

# test procedure
#crime_types = crime_types[[14]]

#####################################
# Until we implement tracking
# Delete all HS tables
#####################################
dbExecute(con, "DELETE FROM [C_TBL_HS_PERS]")
dbExecute(con, "DELETE FROM [C_TBL_HS_OCC]")
dbExecute(con, "DELETE FROM [C_TBL_HS_GEOM]")

print("Generating Kernel Density Estimates (kde) for all Crime Categories")

for (crime_type in crime_types) {
  
  # Create a list to store RasterLayers
  raster_list <- list()
  
  # Filter data for the current crime type
  crime_data <- incidents_sf %>% filter(Category == crime_type)
  
  nocount = 0
  
  # Iterate over 30-day windows
  start_date <- Sys.Date() - 720 # min(crime_data$StartTime)
  end_date <- Sys.Date() - 1 #max(crime_data$StartTime)
  while (start_date <= end_date) {
    # Create a subset for the current time window
    subset_data <- crime_data %>% filter(StartTime >= start_date & StartTime < start_date + time_window)
    
    # if there is data
    if (nrow(subset_data) > 0) {
      kde_result <- kde(subset_data,band_width = 300,kernel="quartic",grid = raster_grid)
    } else {
      kde_result = raster_grid
      values(kde_result) = 0
    }
    
    nocount = nocount + 1
    
    #Convert to SpatRAster
    kde_result = rast(kde_result)
    names(kde_result) = paste(crime_type,nocount,sep = "-")
    time(kde_result) = start_date
    
    # Create RasterLayer using SpatialKDE
    
    #raster_layer <- raster(kde_result, "LP")
    
    # Store the raster layer in the list
    raster_list[[length(raster_list) + 1]] <- kde_result
    
    # Increment the time window
    start_date <- start_date + time_window
    
    print(paste(crime_type,as.character(start_date),"completed..."))
    
  }
  
  #Put all layers in a cube
  dataCube = rast(raster_list)
  print(paste("Building data cube for",crime_type))
  
  zscore = (dataCube - mean(dataCube, na.rm = TRUE))/stdev(dataCube,na.rm = TRUE)
  print(paste("Caculating z-scores for",crime_type))
  
  # Extract the 24th layer
  layer_25 <- zscore[[24]]
  
  # Use terra functions to filter the raster
  filtered_layer <- clamp(layer_25, 1, Inf)
  
  # Set values below 2 to NA
  filtered_layer[layer_25 <= 1] <- NA
  
  #plot(filtered_layer)
  
  # convert to SpatVector then sf
  filtered_layer_sf = as_sf(as.polygons(filtered_layer))
  
  # combine geometries that are touching
  combined_geometries <- st_union(filtered_layer_sf)
  
  # Make them polygons again and find convex hull of each
  anomaly_HS = st_convex_hull(st_cast(combined_geometries, "POLYGON"))
  
  #plot(anomaly_HS)
  
  # Assign a PolyId to each hot spot
  anomaly_HS = st_as_sf(anomaly_HS) %>% mutate(PolyID = row_number())
  
  # Find all occurrences that fall in Hot Spot
  anomaly_HS_OCC_sf = st_intersection(incidents_sf,anomaly_HS)
  
  # Add new column to hold full DateTime
  anomaly_HS_OCC_sf$StartTimeDTS = anomaly_HS_OCC_sf$StartTime
  
  # convert POSIXt StartTime to Date
  anomaly_HS_OCC_sf$StartTime = as.Date(anomaly_HS_OCC_sf$StartTime)
  
  # Calculate the difference in months
  anomaly_HS_OCC_sf$months_ago = as.numeric(difftime(Sys.time(), anomaly_HS_OCC_sf$StartTime, units = "days") / 30)
  
  # Round to the nearest whole number
  anomaly_HS_OCC_sf$months_ago = floor(anomaly_HS_OCC_sf$months_ago)
  
  # Just keep the relevant Category of Occurrences
  anomaly_HS_OCC_sf = anomaly_HS_OCC_sf %>% filter(Category == crime_type)
  
  #################################################
  # recalculate z-score per identified hotspot
  
  # Drop geometry and convert to tibble as group_by with sf works unexpectedly
  # Now we have two data elements, one sf (anomaly_HS_OCC_sf) and one tibble(anomaly_HS_OCC)
  anomaly_HS_OCC = as_tibble(st_drop_geometry(anomaly_HS_OCC_sf))
  
  # convert POSIXt StartTime to Date
  anomaly_HS_OCC$StartTime = as.Date(anomaly_HS_OCC$StartTime)
  
  # Count how many incidents by type occurred in each Hot Spot and Time Period
  suppressMessages({
  group_data_cat = anomaly_HS_OCC %>% group_by(PolyID,months_ago) %>% summarise(total = n()) %>% ungroup()
  })

  # Add missing zeros
  last_24_months = seq(0,24) %>% tibble() %>% rename(months_ago = ".")
  suppressMessages({
  add_zeros_cat = right_join(group_data_cat,last_24_months) %>% complete(PolyID,months_ago) %>% mutate_all(~replace(., is.na(.), 0))
  })
  
  #add_zeros = add_zeros %>% mutate(uniqueCombo = paste(GridID,UCR_Description, sep=""))
  #add_zeros_cat = add_zeros_cat %>% mutate(uniqueCombo = paste(PolyID, sep=""))
  
  # Find z-score per uniquecombo 
  #add_zeros$z = ave(add_zeros$total, add_zeros$uniqueCombo, FUN=scale)
  add_zeros_cat$z = ave(add_zeros_cat$total, add_zeros_cat$PolyID, FUN=scale)
  
  # Find average count per uniquecombo
  #add_zeros$grpave = ave(add_zeros$total, add_zeros$uniqueCombo, FUN=mean)
  add_zeros_cat$grpave = ave(add_zeros_cat$total, add_zeros_cat$PolyID, FUN=mean)
  
  #by_type = add_zeros %>% rename(type = UCR_Description) %>% filter(z > -100)
  by_cat = add_zeros_cat %>% filter(months_ago == 0 & z > 3 & total > 4)
  
  if (nrow(by_cat > 0)){
    
    # Now that z-score calculations are complete, link back to sf incidents
    suppressMessages({
    hs_occ_sf = inner_join(anomaly_HS_OCC_sf,by_cat, by = "PolyID")
    })
  
    # Get confirmed suspects
    # hs_cfm_suspects = inner_join(hs_occ_sf,person_data, by = c("OccurrenceFileNo"))
  
    # Get possible suspects
    hs_pos_suspects = st_join(person_data_sf,anomaly_HS) %>% filter(PolyID %in% unique(hs_occ_sf$PolyID) & Category == crime_type)
    
    # Save results to SILO
    # Save hot spots geometries to HS_GEOM Table
    anomaly_HS_OUT <- anomaly_HS %>% 
      rowwise() %>%
      filter(PolyID %in% unique(by_cat$PolyID)) %>%
      mutate(
        geom = st_as_text(x %>% st_transform(crs = 4326)),
        hs_ID = digest(geom),
        date_generated = Sys.Date(),
        CrimeCat = crime_type
      ) %>% tibble() %>% 
      select(-x)
    
    anomaly_HS_OUT = inner_join(anomaly_HS_OUT,by_cat, by = "PolyID") %>% select(-months_ago)
    
    dbWriteTable(con,"C_TBL_HS_GEOM",anomaly_HS_OUT %>% select(-PolyID) %>% rename(zscore = z),append = TRUE)
    
    # Link to incidents and save to HS_OCC Table
    HS_OCC_OUT = inner_join(anomaly_HS_OUT,hs_occ_sf, by = c("PolyID")) %>% select(hs_ID,Id,Category,latitude,longitude,UCR_Description,StartTimeDTS) %>% rename(occ_id = Id)
    
    dbWriteTable(con,"C_TBL_HS_OCC",HS_OCC_OUT,append = TRUE)
    
    # Link to persons and save to HS_PERS Table
    HS_PERS_OUT = inner_join(anomaly_HS_OUT,hs_pos_suspects, by = c("PolyID")) %>% select(hs_ID,RId) %>% rename(pers_id = RId)
    
    dbWriteTable(con,"C_TBL_HS_PERS",HS_PERS_OUT,append = TRUE)

    if (crime_type == "B & E"){
      print("stop")
    }
    

  }
  
  print(paste(crime_type,"complete, found",nrow(by_cat),"hot spots."))
  print("                            ")
  print("----------------------------")
  print("                            ")
  
}


# Re-instate messages for dev
# options(warn = oldw)
# options(message = TRUE)

#################################################
################# END ###########################
#################################################
