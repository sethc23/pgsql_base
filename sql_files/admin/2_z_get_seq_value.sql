
CREATE OR REPLACE FUNCTION z_get_seq_value
    (
    seq_name text
    )
RETURNS integer
LANGUAGE plpgsql
AS $function$ 
DECLARE x int;
BEGIN
    x = currval(seq_name::regclass)+1;
    return x;
    exception
        when sqlstate '42P01' then return 1;
        when sqlstate '55000' then return next_val(seq_name::regclass);
END;
$function$;