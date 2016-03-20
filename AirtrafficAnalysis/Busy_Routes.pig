A = load '/home/teki/hive/AA.txt' using PigStorage(',') as (Year :int, Month :int, DayofMonth :int, DayOfWeek :int, DepTime :int, CRSDepTime :int,ArrTime :int,CRSArrTime :int,UniqueCarrier :chararray,FlightNum : int,TailNum : chararray,ActualElapsedTime :int,CRSElapsedTime :int,AirTime :int,ArrDelay :int,DepDelay :int,Origin :chararray,Dest : chararray,Distance :int,TaxiIn :int,TaxiOut :int,Cancelled :int,CancellationCode :int,Diverted :int,CarrierDelay :int,WeatherDelay :int,NASDelay : int,SecurityDelay :int,LateAircraftDelay :int);

B = foreach A  generate Year , Origin as f1 , Dest as f2;
C = foreach B generate Year , CONCAT(f1,f2) as f3;
D = Foreach B Generate Year,  CONCAT(f2,f1) as f4;
E = group D by f4;
G = group C by f3;
H = foreach G generate group, COUNT(C) as r1;
F = foreach E generate group, COUNT(D) as r2;
I = UNION H,F;


N = Foreach I generate (r1+r2) as v;
O = order N by v desc;
T = limit O 20;
dump T;


