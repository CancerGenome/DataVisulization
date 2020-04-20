# CSE6242_GroupProject
## Processing Data
### Download
Insider Airbnb:  http://insideairbnb.com/get-the-data.html
Mashvisor: Postman_Mashvisor API.docx
Census: 
Crime data:


### Integration
Convert Mashvisor Json to csv with json2csv and define the selected field with the following command line.
Problems: region1 and 5 are one line json file and json2csv can recognize;
Region 3 have additional comma after each json file, should remove first;
Region 6 have removed content header, therefore we should remove all content_ in following command line;
For all files, replace all <p></p> \r and \n to avoid malformatted results;

Merge all census and mashvisor and InsiderAirbnb with in-house R-script (see below). 
Have prepared a detailed subtype list for crime, but didn’t add to our final CSV because possible overwhelming columns (300+ more columns)

### Data Cleaning

Clean the data and binary or categorize the dataset, with the reference from Mashvisor. See comments for header and find the solution for each dataset. All red labelled headers are selected for next step analysis. 


## Model 1
## Model 2
