CREATE OR REPLACE FUNCTION z_pdf_pgcnt
	(
	text
	)
RETURNS integer
LANGUAGE plshu
AS $function$#!/bin/bash
/usr/bin/pdfinfo "$1" 2> /dev/null|grep "Pages: "|sed "s/Pages: *//"
$function$;