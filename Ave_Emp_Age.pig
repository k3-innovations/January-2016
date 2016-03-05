/*
 * Pig Script begines here...
 *   
*/

emp = LOAD '/home/ubuntu/employee_db/employees/part-m-00000' USING PigStorage(',') AS (emp_no:int, birth_date:chararray, first_name:chararray, last_name:chararray, gender:chararray );
dept = LOAD '/home/ubuntu/employee_db/departments/part-m-00000' USING PigStorage(',') AS (dept_no:chararray, dept_name:chararray );
dept_emp = LOAD '/home/ubuntu/employee_db/dept_employees/part-m-00000'  USING PigStorage(',') AS (emp_no:int, dept_no:chararray);

/*
 * Define macro for setting data limits...
 *   
*/

DEFINE set_limits (load_class, limit_no) RETURNS set_limit_val {
    $set_limit_val = LIMIT $load_class $limit_no;
};

/*
 * Set limits for testing...Here we set 5 lines of data to test...
 *   
*/

emp = set_limits(emp,5);
dept_emp = set_limits(dept_emp,5);
age_emp = FOREACH emp GENERATE emp_no, YearsBetween(CurrentTime(),ToDate(birth_date, 'YYYY-MM-DD')) AS age, first_name, last_name, gender;


/*
 * Joining teh tables. First inner join of dept and dept_emp to capture the department names.
 * Follwowed by inner join with employees table
*/
dept_join = JOIN dept_emp BY dept_no, dept by dept_no;
describe dept_join;
--dept_join: {dept_emp::emp_no: int,dept_emp::dept_no: chararray,dept::dept_no: chararray,dept::dept_name: chararray}


emp_join = JOIN dept_join BY dept_emp::emp_no, age_emp BY emp_no;
describe emp_join;
--emp_join: {dept_join::dept_emp::emp_no: int,dept_join::dept_emp::dept_no: chararray,dept_join::dept::dept_no: chararray,dept_join::dept::dept_name: chararray,age_emp::emp_no: int,age_emp::age: long,age_emp::first_name: chararray,age_emp::last_name: chararray,age_emp::gender: chararray}

/*
* Here we do a Grouping of the joined tables by dept_name
*/

dept_group = GROUP emp_join BY (dept::dept_name,dept::dept_no);
describe dept_group;
--{group: (dept_join::dept::dept_name: chararray,dept_join::dept::dept_no: chararray),emp_join: {(dept_join::dept_emp::emp_no: int,dept_join::dept_emp::dept_no: chararray,dept_join::dept::dept_no: chararray,dept_join::dept::dept_name: chararray,age_emp::emp_no: int,age_emp::age: long,age_emp::first_name: chararray,age_emp::last_name: chararray,age_emp::gender: chararray)}}

/*
 * This is where the averaging occurs, by using the group and calling a UDF
 *   
*/

ave_age = FOREACH dept_group GENERATE (group.dept_join::dept::dept_name), AVG(emp_join.age_emp::age);

describe ave_age;
--ave_age: {dept_join::dept::dept_name: chararray,double}

STORE ave_age INTO '/home/ubuntu/Pig_Output' USING PigStorage(',');

/*
* End of Program.
*
*/




