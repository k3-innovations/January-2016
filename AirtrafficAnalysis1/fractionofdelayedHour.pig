Airport_Relation  = load '/home/teki/hive/airportdata.txt' using PigStorage(',') as (Year :int, Month :int, DayofMonth :int, DayOfWeek :int, 		 						DepTime :int, CRSDepTime :int,ArrTime :int,CRSArrTime :int,UniqueCarrier :chararray,FlightNum : int,TailNum : 		 	
					chararray,ActualElapsedTime :int,CRSElapsedTime :int,AirTime :int,ArrDelay :int,DepDelay :int,Origin :chararray,Dest : 	 
					chararray,Distance :int,TaxiIn :int,TaxiOut :int,Cancelled :int,CancellationCode :int,Diverted :int,CarrierDelay :int, 	
					WeatherDelay :int,NASDelay : int,SecurityDelay :int,LateAircraftDelay :int);

Delay_Relation = foreach Airport_Relation  generate  CRSDepTime  as depatime , DepDelay as delay;

Group_delay = group Delay_Relation by  depatime;


Count_delay = FOREACH Group_delay {
			Filter_delay = FILTER Delay_Relation BY (delay >= 15); 
			GENERATE group,  (double)COUNT(Filter_delay)/COUNT(Delay_Relation) AS frachr;
			};
Order_delay= order Count_delay by frachr desc;
Top_delay= limit Order_delay 15; 

Dump Top_delay;
 
  
