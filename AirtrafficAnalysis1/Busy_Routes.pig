Airport_Relation  = load '/home/teki/hive/airportdata.txt' using PigStorage(',') as (Year :int, Month :int, DayofMonth :int, DayOfWeek :int, 		 						DepTime :int, CRSDepTime :int,ArrTime :int,CRSArrTime :int,UniqueCarrier :chararray,FlightNum : int,TailNum : 		 	
					chararray,ActualElapsedTime :int,CRSElapsedTime :int,AirTime :int,ArrDelay :int,DepDelay :int,Origin :chararray,Dest : 	 
					chararray,Distance :int,TaxiIn :int,TaxiOut :int,Cancelled :int,CancellationCode :int,Diverted :int,CarrierDelay :int, 	
					WeatherDelay :int,NASDelay : int,SecurityDelay :int,LateAircraftDelay :int);

Route_Relation = foreach Airport_Relation  generate Year , Origin  , Dest ;
group_route= group Route_Relation by (Origin,Dest);

Count_route = Foreach group_route generate group, COUNT(Route_Relation.Origin) as cnt;
Order_route = order Count_route by cnt desc;
Top_BusyRoute = limit Order_route 20;
dump Top_BusyRoute;


