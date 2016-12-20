CREATE OR REPLACE FUNCTION z_pdftotext
	(
	text
	)
RETURNS text
LANGUAGE plshu
AS $function$#!/bin/bash
pdftotext -q -nopgbrk $1 -
$function$;