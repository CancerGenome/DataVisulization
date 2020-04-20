# CSE6242_GroupProject
## Processing Data
### Download
In total, we downloaded four different datasets: 
Airbnb renting information from Insider Airbnb;
On sale property from Mashvisor;
Census information and detailed Crime data from Census American Community Survey (ACS);

The on-sale property was downloaded through Mashvisor API and all other datasets are available for direct download. 


### Data Cleaning and Integration
We cleaned, converted, and integrate our dataset through following steps:
The crime data were aggregated by zip code and total crime number was reported for each zip code;
The census data was converted to binary by comparing it with the global median value. 1 means higher, otherwise 0. 
The census zipcode were converted to category dataset. 
Convert Mashvisor Json to csv with json2csv and define the selected field to output
All binary data were converted into 1/0. 
All amenities in the aribnb were converted into category data.

After all of these, we combined the census, crime data with Airbnb and Mashvisor data through zipcode. All cleaning procedures were finished in R. 

## Model 1
## Model 2
