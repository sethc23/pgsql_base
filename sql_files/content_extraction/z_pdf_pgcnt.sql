CREATE OR REPLACE FUNCTION z_pdf_pgcnt
	(
	text
	)
RETURNS integer
LANGUAGE plshu
AS $function$#!/bin/bash
#/usr/bin/pdfinfo "$1" 2> /dev/null|grep "Pages: "|sed "s/Pages: *//"
/usr/bin/pdfinfo "$1" 2> /dev/null|xargs --null echo|env grep 'Pages:'|sed -r 's/^(.*)Pages:[ ]+([0-9,]+)(.*)$/\2/'
$function$;