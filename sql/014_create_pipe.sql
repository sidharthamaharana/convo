create or replace pipe employee_pipe as
copy into employee
from @%empemployee;
