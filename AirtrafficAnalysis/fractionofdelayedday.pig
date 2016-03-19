	A = load '/home/teki/hive/airportdata.txt' using PigStorage(',') as (Year :int, Month :int, DayofMonth :int, DayOfWeek :int, DepTime :int, CRSDepTime :int,ArrTime :int,CRSArrTime :int,UniqueCarrier :chararray,FlightNum : int,TailNum : chararray,ActualElapsedTime :int,CRSElapsedTime :int,AirTime :int,ArrDelay :int,DepDelay :int,Origin :chararray,Dest : chararray,Distance :int,TaxiIn :int,TaxiOut :int,Cancelled :int,CancellationCode :int,Diverted :int,CarrierDelay :int,WeatherDelay :int,NASDelay : int,SecurityDelay :int,LateAircraftDelay :int);

	  
		B = foreach A generate  DayofMonth as day , DepDelay as delay;

		C = group B by day;


		D = FOREACH C {
		E = FILTER B BY (delay >= 15); 
		GENERATE group,  (double)COUNT(E)/COUNT(B) AS fracdy;
		};
	F= order D by fracdy desc;
	G= limit F 15; 
	 
	Dump G;

