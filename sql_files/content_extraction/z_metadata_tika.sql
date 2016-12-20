CREATE OR REPLACE FUNCTION z_metadata_tika
	(
	text
	)
RETURNS text
LANGUAGE plshu
AS $function$ 
#!/bin/bash
/usr/local/bin/tika_meta $1 2> /dev/null
$function$;