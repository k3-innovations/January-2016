A= Load '/home/teki/cust/cust.txt' using PigStorage(',') as (date:chararray,product:chararray,subpro:chararray,company:chararray,State:chararray);
D = Foreach A generate ToDate(date,'MM/dd/yyyy') as date ,product; 
G = group D by product;  
I = foreach G {
m= filter D by (date <=ToDate('2015-11-30') and date >=ToDate('2015-11-01'));
k= filter D by (date <=ToDate('2015-12-31') and date >=ToDate('2015-12-01'));
generate group, COUNT(k)as val, (double)(COUNT(k)-COUNT(m))*100/COUNT(m) ,
COUNT(D)/MonthsBetween(MAX(D.date),MIN(D.date)) ,COUNT(D);
};
T = order I by val desc;
N = limit T 10;
dump N;
