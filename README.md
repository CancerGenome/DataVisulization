# CSE6242_GroupProject
## Processing Data
### Data collection and scraping
In total, we downloaded four different datasets: 

Airbnb renting information from [Insider Airbnb](http://insideairbnb.com/get-the-data.html) ;

On sale property from [Mashvisor](https://www.mashvisor.com/);

Census information and detailed Crime data from Census [American Community Survey ACS](https://www.census.gov/programs-surveys/acs/data.html) and [CDE](https://crime-data-explorer.fr.cloud.gov/downloads-and-docs);

The on-sale property was downloaded through Mashvisor API and all other datasets are available for direct download. 

### Download Mashvisor data
We first explored all on-sale property and their home ID from Mashvisor. Then we downloded all information through API with the command below. (Caveat: we registered first to get the api key and each api key only have  

```bash
curl --location --request GET "https://api.mashvisor.com/v1.1/client/property?id=$ID&state=TX" --header "x-api-key: 3e2c07a8-ce42-48cb-bd29-6ad797326a0e 
```

### Data cleaning and integration
We cleaned, converted, and integrate our dataset through following steps:

The crime data were aggregated by zip code and total crime number was reported for each zip code;

The census data was converted to binary by comparing it with the global median value. 1 means higher, otherwise 0. 

The census zipcode were converted to category dataset. 

Convert Mashvisor Json to csv with [json2csv](https://www.npmjs.com/package/json2csv) and define the selected field to output

All binary data were converted into 1/0. 

All amenities in the aribnb were converted into category data.

After all of these, we combined the census, crime data with Airbnb and Mashvisor data through zipcode. All cleaning procedures were finished in R. 

### Dependency
R library:

reshape

fastDummies

## Model 1
## Model 2
