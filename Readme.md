### Program Structure  

The program is divided into 2 Map-Reduce jobs.
 
#### Job 1: Find top 5 Airlines and Airports  
##### Map - TopAirlineAirportMap.ActiveAirportAirlinesMapper:
  - Reads the input CSV and counts the flight frequency per airline and per airport. String "0" or "1" is appended to the keys, 0 for Airlines and 1 for Aiports.

##### Partition - TopAirlineAirportMap.TopPartitioner:
  - Partitions the key from the mapper into two different reducers by using "0" and "1" appended to the keys.

##### Reduce - TopAirlineAirportReducer:
  - Receives airline/airport as key and list of frequncies as value.
  - Adds the frequency value and calculates the total frequency count of each airline and airport.  
  
    
#### Job 2: Calculate monthly flight delay
##### Map - CalculateMonthlyDelay.AverageMonthlyDelayMapper:
  - Reads the output of job 1, sorts it and finds the top 5 airlines and airports.
  - Then map function in the first stage reads each record and pchecks if it passes the sanity test and then sets it to the approriate Airline or Airport hashmap based on the calculated top 5 airport and airline.

##### Partition - CalculateMonthlyDelay.AverageMonthlyPartitioner:
  - Partitions each airport or airline by the month (will result in 24 reducers)  
  - The partitioner differentiates the record between airport or airline by using the first character in the key which is either "0" or "1" and finds out the month which is the 4th element in the key.  

##### Reduce - MonthlyDelayReducer:
  - Aggregrates all the normalized delays for each airport and airline and finds the mean normalized delay for the same. The result is written in a CSV file.  
  

