Airport_Relation = load '/home/teki/hive/airportdata.txt' using PigStorage(',') as (Year :int, Month :int, DayofMonth :int, DayOfWeek :int, DepTime :int, CRSDepTime :int,ArrTime :int,CRSArrTime :int,UniqueCarrier :chararray,FlightNum : int,TailNum : chararray,ActualElapsedTime :int,CRSElapsedTime :int,AirTime :int,ArrDelay :int,DepDelay :int,Origin :chararray,Dest : chararray,Distance :int,TaxiIn :int,TaxiOut :int,Cancelled :int,CancellationCode :int,Diverted :int,CarrierDelay :int,WeatherDelay :int,NASDelay : int,SecurityDelay :int,LateAircraftDelay :int);

delay_week = foreach Airport_Relation generate DayOfWeek as dayWeek, DepDelay as delay;
filter_delay= filter delay_week by (delay IS NOt Null);--filter out null values
Order_delay = Order filter_delay by delay asc;
Top_20 = limit Order_delay 20; 
dump Top_20;

