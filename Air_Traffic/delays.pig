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

air_count = FOREACH (GROUP air_traffic ALL) GENERATE COUNT(air_traffic);

-- delays_year = FOREACH air_traffic GENERATE Year, (ArrDelay>15?1:0) AS delay;
delays_year = FILTER air_traffic BY (ArrDelay>15);
delays_count = FOREACH (GROUP delays_year ALL) GENERATE COUNT(delays_year);
-- year_proportion = delays_count/air_count;

-- STORE delays_year INTO 'airport_traffic/delay_year';
-- year_grp = GROUP delays_year BY delay;
STORE delays_count INTO 'airport_traffic/delay_year';

-- year_count = FOREACH year_grp GENERATE group, COUNT(delays_year) AS total;

-- STORE year_count INTO 'airport_traffic/delay_year';