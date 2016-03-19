A= Load '/home/teki/cust/cust.txt' using PigStorage(',') as (date:chararray,product:chararray,subpro:chararray,company:chararray,State:chararray);
D = Foreach A generate ToDate(date,'MM/dd/yyyy') as date ,State; 
G = group D by State;  
I = foreach G {
m= filter D by (date <=ToDate('2014-12-31') and date >=ToDate('2014-10-01'));
k= filter D by (date <=ToDate('2015-12-31') and date >=ToDate('2015-10-01'));
generate group, 100*(float)(COUNT(k)-COUNT(m))/COUNT(m) as val,COUNT(m)/3, COUNT(k)/3,COUNT(D);
};
T = order I by val desc;
N = limit T 20;
dump N;

