library(reshape)
library(fastDummies)
##### Read in files 
Crime <- read.csv("Census/Crime_Reports_2018.csv")
Neighboorhood <- read.csv("Census/Neigborhood.csv", stringsAsFactors = F)
Mashvisor <- read.csv("Mashvisor_Json/All.csv")
InsiderAirbnb <- read.csv("InsiderAirbnb/listing.detailed.clean.csv")

##### Aggregate Crime data
Crime_Agg_By_ZipCode_Category = aggregate(Crime$Zip.Code, by = list(Crime$Zip.Code, Crime$Highest.Offense.Description), length)
Crime_Total_ZipCode = aggregate(Crime$Zip.Code, by = list(Crime$Zip.Code), length)
colnames(Crime_Agg_By_ZipCode_Category) = c("ZipCode", "Crime_Subtype","Crime_Subtype_Count")
colnames(Crime_Total_ZipCode) = c("ZipCode", "Crime_Total_Count")
Crime_ZipCode_Subtype = cast(Crime_Agg_By_ZipCode_Category, ZipCode~Crime_Subtype, length)

##### Start to Merge
Merge_Crime_Census <- merge(Crime_Total_ZipCode, Neighboorhood, by.x = "ZipCode", by.y = "Zipcode", all = T)
#Merge_Crime_Census <- merge(Merge_Crime_Census1, Crime_ZipCode_Subtype, by = "ZipCode", all = T)

Merge_Mashvisor <- merge(Merge_Crime_Census, Mashvisor, by.x = 'ZipCode', by.y = "zip", all.x =F, all.y = T)
Merge_InsiderAirbnb <- merge(Merge_Crime_Census, InsiderAirbnb, by.x = 'ZipCode', by.y = "zipcode", all.x=F, all.y = T)

#### Write Output
write.csv(Merge_Mashvisor, file = "Mashvisor_Json/Mashvisor_withCensus_Crime.csv")
write.csv(Merge_InsiderAirbnb, file = "InsiderAirbnb/listing_withCensus_Crime.csv")

#######---- V2: This is further cleaning --------
##### Process datasets, binary, categories or others for Mashvisor Only
M <- Merge_Mashvisor
A <- Merge_InsiderAirbnb
N <- Neighboorhood
N <- N[N$Zipcode %in% M$ZipCode,]

M <- fastDummies::dummy_cols(M, select_columns = "ZipCode")
M$Crime_Total_Count <- ifelse(is.na(M$Crime_Total_Count), 0, M$Crime_Total_Count)

#dim(Neighboorhood)
new_sum <- function(x){return(sum(as.numeric(x),na.rm=T)) }
new_median <- function(x){return(median(as.numeric(x),na.rm=T)) }
neighboor_sum = apply(N[3:14],2,new_sum)
neighboor_mean = neighboor_sum/neighboor_sum["Population2017"]
neighboor_median = unlist(apply(N[3:14],2,new_median))


M$Male2017 <- ifelse(M$Male2017/M$Population2017>= neighboor_mean["Male2017"],1,0 )
M$Female2017 <- ifelse(M$Female2017/M$Population2017>= neighboor_mean["Female2017"],1,0 )
M$Bachelor_total17 <- ifelse(M$Bachelor_total17/M$Population2017 >= neighboor_mean["Bachelor_total17"],1,0 )
M$Hispanic17 <- ifelse(M$Hispanic17/M$Population2017>= neighboor_mean["Hispanic17"],1,0 )
M$White17 <- ifelse(M$White17/M$Population2017>= neighboor_mean["White17"],1,0 )
M$Black17 <- ifelse(M$Black17/M$Population2017>= neighboor_mean["Black17"],1,0 )
M$Asian17 <- ifelse(M$Asian17/M$Population2017>= neighboor_mean["Asian17"],1,0 )

M$PerCapital_income17 <- ifelse(as.numeric(as.character(unlist(M$PerCapital_income17))) >= as.numeric(as.character(unlist(neighboor_median["PerCapital_income17"]))),1,0)
M$MedHomeValue17 <- ifelse(as.numeric(as.character(unlist(M$MedHomeValue17))) >= as.numeric(as.character(unlist(neighboor_median["MedHomeValue17"]))),1,0)
M$Med_House_income17 <- ifelse(as.numeric(as.character(unlist(M$Med_House_income17))) >= as.numeric(as.character(unlist(neighboor_median["Med_House_income17"]))),1,0)
M$Belov_Poverty_percent17 <- ifelse(as.numeric(as.character(unlist(M$Belov_Poverty_percent17))) >= as.numeric(as.character(unlist(neighboor_median["Belov_Poverty_percent17"]))),1,0)

M$yearBuilt <- 2021 - M$yearBuilt
M$neighborhood_is_village = as.numeric(M$neighborhood_is_village)

M$homeType = gsub(" ","_", M$homeType)
M <- fastDummies::dummy_cols(M, select_columns = "homeType")
M$homeType = gsub(" ","_", M$property_type)
M <- fastDummies::dummy_cols(M, select_columns = "property_type")

M$parkingType <- ifelse(M$parkingType == "", 0,1)
M$owner_occupied <- as.numeric(M$owner_occupied)
M$heating_system <- ifelse(M$heating_system == "", 0,1)
M$cooling_system <- ifelse(M$cooling_system == "", 0,1)

M$architecture_style = gsub(" ","_", M$architecture_style)
M <- fastDummies::dummy_cols(M, select_columns = "architecture_style")

M$has_pool = ifelse(is.na(M$has_pool),0,1)
M$is_water_front = ifelse(is.na(M$is_water_front),0,1)
M$Elementary = ifelse(M$schools_0_name == "",0,1)
M$High = ifelse(M$schools_1_name == "",0,1)
M$JuniorHigh = ifelse(M$schools_2_name == "",0,1)
M$Middle = ifelse(M$schools_3_name == "",0,1)
M[is.na(M)] = 0 
write.csv(M, file = "Mashvisor_Json/Mashvisor_withCensus_CrimeV2.csv")
save(file="Mashvisor.RData", list = "M")

#### Done with Mashvisor Reformating

####_-------------- For Airbnb Only ---------
A <- Merge_InsiderAirbnb
N <- Neighboorhood
N <- N[N$Zipcode %in% A$ZipCode,]

A <- fastDummies::dummy_cols(A, select_columns = "ZipCode")
A$Crime_Total_Count <- ifelse(is.na(A$Crime_Total_Count), 0, A$Crime_Total_Count)

#dim(Neighboorhood)
new_sum <- function(x){return(sum(as.numeric(x),na.rm=T)) }
new_median <- function(x){return(median(as.numeric(x),na.rm=T)) }
neighboor_sum = apply(N[3:14],2,new_sum)
neighboor_mean = neighboor_sum/neighboor_sum["Population2017"]
neighboor_median = unlist(apply(N[3:14],2,new_median))

A$Male2017 <- ifelse(A$Male2017/A$Population2017>= neighboor_mean["Male2017"],1,0 )
A$Female2017 <- ifelse(A$Female2017/A$Population2017>= neighboor_mean["Female2017"],1,0 )
A$Bachelor_total17 <- ifelse(A$Bachelor_total17/A$Population2017 >= neighboor_mean["Bachelor_total17"],1,0 )
A$Hispanic17 <- ifelse(A$Hispanic17/A$Population2017>= neighboor_mean["Hispanic17"],1,0 )
A$White17 <- ifelse(A$White17/A$Population2017>= neighboor_mean["White17"],1,0 )
A$Black17 <- ifelse(A$Black17/A$Population2017>= neighboor_mean["Black17"],1,0 )
A$Asian17 <- ifelse(A$Asian17/A$Population2017>= neighboor_mean["Asian17"],1,0 )

A$PerCapital_income17 <- ifelse(as.numeric(as.character(unlist(A$PerCapital_income17))) >= as.numeric(as.character(unlist(neighboor_median["PerCapital_income17"]))),1,0)
A$MedHomeValue17 <- ifelse(as.numeric(as.character(unlist(A$MedHomeValue17))) >= as.numeric(as.character(unlist(neighboor_median["MedHomeValue17"]))),1,0)
A$Med_House_income17 <- ifelse(as.numeric(as.character(unlist(A$Med_House_income17))) >= as.numeric(as.character(unlist(neighboor_median["Med_House_income17"]))),1,0)
A$Belov_Poverty_percent17 <- ifelse(as.numeric(as.character(unlist(A$Belov_Poverty_percent17))) >= as.numeric(as.character(unlist(neighboor_median["Belov_Poverty_percent17"]))),1,0)

A$bed_type = gsub(" ","_", A$bed_type)
A <- fastDummies::dummy_cols(A, select_columns = "bed_type")

A$price = gsub("\\$","", A$price)
A$price = gsub(",","", A$price)
A$security_deposit = gsub("\\$","", A$security_deposit)
A$security_deposit = gsub(",","", A$security_deposit)
A$cleaning_fee = gsub("\\$","", A$cleaning_fee)
A$cleaning_fee = gsub(",","", A$cleaning_fee)
A$extra_people = gsub("\\$","", A$extra_people)
A$extra_people = gsub(",","", A$extra_people)

# prepare the amenities sub categories. 
A$amenities <- gsub(" ","_", A$amenities)
A$amenities <- gsub("\\{","", A$amenities)
A$amenities <- gsub("\\}","", A$amenities)
A$amenities <- gsub("\"" ,"", A$amenities)

Split_Ame <- sort(unique(unlist(strsplit(as.character(A$amenities),split = ","))))
i = 1 
for(i in 1:length(Split_Ame)){
#for(i in 1:2){
  A[paste("Amenities",Split_Ame[i],sep="_")] <- as.numeric(grepl(Split_Ame[i],A$amenities))
}
A[is.na(A)] = 0 
write.csv(A, file = "InsiderAirbnb/listing_withCensus_CrimeV2.csv")
save(file="Airbnb.RData", list = "A")

