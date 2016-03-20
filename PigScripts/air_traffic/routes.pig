air_traffic = LOAD 'air_traffic/input/2008.csv' USING PigStorage(',') 
			AS(Year:chararray,
				Month:chararray,
				DayofMonth:chararray,
				DayofWeek:chararray,
				DepTime:chararray,
				CRSDepTime:chararray,
				ArrTime:chararray,
				CRSArrTime:chararray,
				UniqueCarrier:chararray,
				FlightNum:chararray,
				TailNum:chararray,
				ActualElapsedTime:chararray,
				CRSElapsedTime:chararray,
				AirTime:chararray,
				ArrDelay:int,
				DepDelay:chararray,
				Origin:chararray,
				Dest:chararray,
				Distance:chararray,
				TaxiIn:chararray,
				TaxiOut:chararray,
				Cancelled:chararray,
				CancellationCode:chararray,
				Diverted:chararray,
				CarrierDelay:chararray,
				WeatherDelay:chararray,
				NASDelay:chararray,
				SecurityDelay:chararray,
				LateAircraftDelay:chararray);

routes = FOREACH air_traffic GENERATE CONCAT(Origin, Dest) as route, 1 AS count;
routes_grp = GROUP routes BY route;
routes_count = FOREACH routes_grp GENERATE group as route, COUNT(routes) as counts;
routes_totals = FOREACH routes_count GENERATE route, counts;
sorted_results = ORDER routes_totals BY counts DESC; 

-- STORE routes_totals INTO 'air_traffic/output/routes';
STORE sorted_results INTO 'air_traffic/output/routes';