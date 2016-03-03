employees = LOAD 'employees' USING PigStorage(',') AS (emp_no:int, birth_date:datetime, first_name:chararray, last_name:chararray, gender:chararray, hire_date:datetime);
dept_emp = LOAD 'dept_emp' USING PigStorage(',') AS (emp_no:int, dept_no:chararray, from_date:datetime, to_date:datetime);
departments = LOAD 'departments' USING PigStorage(',') AS (dept_no:chararray, dept_name:chararray);


empAge = FOREACH employees GENERATE emp_no, YearsBetween(CurrentTime(), birth_date) AS age;
deptData = FOREACH (JOIN dept_emp BY emp_no, empAge BY emp_no) GENERATE dept_emp::dept_no, empAge::age;

deptDataGrp = GROUP deptData BY dept_no;
deptAge = FOREACH deptDataGrp GENERATE group, AVG(deptData.age) as avg;

final = FOREACH (JOIN deptAge BY group, departments BY dept_no) GENERATE departments::dept_name, deptAge::avg;


STORE final INTO 'dept_avg_age';

-- DUMP final;
-- DESCRIBE deptData;
-- DUMP empAge;
-- DESCRIBE empAge;
-- STORE empAge INTO 'dept_data';