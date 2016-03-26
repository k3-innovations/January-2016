Complaint_Relation= Load '/home/teki/cust/cust.txt' using PigStorage(',') as   	
      				(date:chararray,product:chararray,subpro:chararray,company:chararray,State:chararray);

CompProduct_Relation = Foreach Complaint_Relation generate ToDate(date,'MM/dd/yyyy') as date ,product; 

Group_CompProduct = group CompProduct_Relation by product;  

Count_CompProduct = foreach Group_CompProduct {
										--complaint by state from Oct-Dec 2014
									CompProduct_lastyr= filter CompProduct_Relation by (date <=ToDate('2014-12-31') and date >=ToDate('2014-10-01'));
                                         --complaint by state from Oct-Dec 2015
									CompProduct_curryr= filter CompProduct_Relation by (date <=ToDate('2015-12-31') and date >=ToDate('2015-10-01'));
									-- change%,avg in 2014, avg in 2015 and total complaint by state
generate group, 100*(float)(COUNT(CompProduct_curryr)-COUNT(CompProduct_lastyr))/COUNT(CompProduct_lastyr) as val,COUNT(CompProduct_lastyr)/3,
COUNT(CompProduct_lastyr)/3,COUNT(CompProduct_Relation);
									};
Order_CompProduct = order Count_CompProduct by val desc;
Top_CompProduct = limit Order_CompProduct 20;
dump Top_CompProduct;

