CREATE OR REPLACE FUNCTION z_readfile
	(
	fpath text
	)
RETURNS text
LANGUAGE plpythonu
AS $function$ 
    with open(fpath,'r') as f: return f.read()
$function$;