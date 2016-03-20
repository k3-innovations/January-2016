air_traffic = LOAD 'airport_traffic/input/2008.csv' USING PigStorage(',') 
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
				ArrDelay:chararray,
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

carrier_group = GROUP air_traffic BY UniqueCarrier;
carrier_count = FOREACH carrier_group GENERATE group as carrier, COUNT(air_traffic) as vol;
carrier_vol = FOREACH carrier_count GENERATE carrier, vol;

STORE carrier_vol INTO 'airport_traffic/carrier';