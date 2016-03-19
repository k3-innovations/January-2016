A = load '/home/teki/hive/airportdata.txt' using PigStorage(',') as (Year :int, Month :int, DayofMonth :int, DayOfWeek :int, DepTime :int, CRSDepTime :int,ArrTime :int,CRSArrTime :int,UniqueCarrier :chararray,FlightNum : int,TailNum : chararray,ActualElapsedTime :int,CRSElapsedTime :int,AirTime :int,ArrDelay :int,DepDelay :int,Origin :chararray,Dest : chararray,Distance :int,TaxiIn :int,TaxiOut :int,Cancelled :int,CancellationCode :int,Diverted :int,CarrierDelay :int,WeatherDelay :int,NASDelay : int,SecurityDelay :int,LateAircraftDelay :int);

B = foreach A generate DayOfWeek as dayWeek, DepDelay as delay;
C= filter B by (delay IS NOt Null);
D = Order C by delay asc;
E = limit D 60; 
dump E;

(DepTime-CRSDepTime)
