Complaint_Relation= Load '/home/teki/cust/cust.txt' using PigStorage(',') as   	
      				(date:chararray,product:chararray,subpro:chararray,company:chararray,State:chararray);

CompState_Relation = Foreach Complaint_Relation generate ToDate(date,'MM/dd/yyyy') as date ,State; 

Group_CompState = group CompState_Relation by State;  

Count_CompState = foreach Group_CompState {
										--complaint by state from Oct-Dec 2014
									CompState_lastyr= filter CompState_Relation by (date <=ToDate('2014-12-31') and date >=ToDate('2014-10-01'));
                                         --complaint by state from Oct-Dec 2015
									CompState_curryr= filter CompState_Relation  by (date <=ToDate('2015-12-31') and date >=ToDate('2015-10-01'));
									-- change%,avg in 2014, avg in 2015 and total complaint by state
									generate group, 100*(float)(COUNT(CompState_curryr)-COUNT(CompState_lastyr))/COUNT(CompState_lastyr)
                                                   as val,COUNT (CompState_lastyr)/3, COUNT(CompState_curryr)/3,COUNT(CompState_Relation);
									};
Order_CompState = order Count_CompState by val desc;
Top_CompState = limit Order_CompState 20;
dump Top_CompState;

