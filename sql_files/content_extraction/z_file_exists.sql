CREATE OR REPLACE FUNCTION public.z_file_exists
	(
	fpath text
	)
RETURNS boolean
LANGUAGE plpythonu
AS $function$
import os
return os.path.exists(fpath)
$function$;