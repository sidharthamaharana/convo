create or replace pipe emp_pipe as
copy into employee
from @%employee;
