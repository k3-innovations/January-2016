Complaint_Relation= Load '/home/teki/cust/cust.txt' using PigStorage(',') as   	
      				(date:chararray,product:chararray,subpro:chararray,company:chararray,State:chararray);

CompCompany_Relation = Foreach Complaint_Relation generate ToDate(date,'MM/dd/yyyy') as date ,company; 

Group_CompCompany = group CompCompany_Relation by company;  

Count_CompCompany = foreach Group_CompCompany {
										--complaint by company from Oct-Dec 2014
									CompCompany_lastyr= filter CompCompany_Relation by (date <=ToDate('2014-12-31') and date >=ToDate('2014-10-01'));
                                         --complaint by comapny from Oct-Dec 2015
									CompCompany_curryr= filter CompCompany_Relation  by (date <=ToDate('2015-12-31') and date >=ToDate('2015-10-01'));
									-- change%,avg in 2014, avg in 2015 and total complaint by state
							generate group, COUNT (CompCompany_lastyr)/3 as val,100*(float)(COUNT(CompCompany_curryr)-COUNT(CompCompany_lastyr))/COUNT(CompCompany_lastyr),COUNT(CompCompany_Relation)/MonthsBetween(MAX(CompCompany_Relation.date),MIN(CompCompany_Relation.date)),COUNT(CompCompany_Relation);
									};
Order_CompCompany = order Count_CompCompany by val desc;
Top_CompCompany = limit Order_CompCompany 20;
dump Top_CompCompany;

