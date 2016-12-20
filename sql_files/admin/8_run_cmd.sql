CREATE OR REPLACE FUNCTION z_run_cmd
	(
	text
	)
RETURNS text
LANGUAGE plshu
AS $function$ 
#!/bin/bash
$1
$function$;