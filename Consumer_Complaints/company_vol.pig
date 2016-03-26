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

company = GROUP complaints BY Company_Name;
company_count = FOREACH company GENERATE group as company, COUNT_STAR(complaints) as cnt;
result = FOREACH company_count GENERATE company, cnt;

STORE result INTO 'complaints/output/company_vol';