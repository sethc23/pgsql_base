

-- DROP FUNCTION z_get_seq_value(text);
DROP FUNCTION IF EXISTS z_get_seq_value(text);
CREATE OR REPLACE FUNCTION z_get_seq_value(seq_name text)
  RETURNS integer AS
$BODY$
declare x int;
begin
x = currval(seq_name::regclass)+1;
return x;
exception
    when sqlstate '42P01' then return 1;
    when sqlstate '55000' then return next_val(seq_name::regclass);
end;
$BODY$
  LANGUAGE plpgsql
