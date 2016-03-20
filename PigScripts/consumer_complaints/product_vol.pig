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

products = GROUP complaints BY Product_Type;
product_count = FOREACH products GENERATE group as product, COUNT_STAR(complaints) as cnt;
result = FOREACH product_count GENERATE product, cnt;

STORE result INTO 'complaints/output/product_vol';