complaints = LOAD 'complaints/input/consumer_complaints.csv' USING PigStorage(',')
			 AS(Date_Recd:chararray,
				Product_Type:chararray,
				Sub_Product:chararray,
				Issue_Type:chararray,
				Sub_Issue:chararray,
				Consumer_Complaint:chararray,
				Company_Response:chararray,
				Company_Name:chararray,
				State:chararray,
				zip_code:chararray,
				Submitted_via:chararray,
				Company_sent_date:datetime,
				company_response:chararray,
				timely_response:chararray,
				consumer_disputed:chararray,
				complaint_id:chararray);

states = GROUP complaints BY State;
states_count = FOREACH states GENERATE group as state, COUNT_STAR(complaints) as cnt;
result = FOREACH states_count GENERATE state, cnt;

STORE result INTO 'complaints/output/states_vol';