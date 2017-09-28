CREATE OR REPLACE FUNCTION public.z_extract_pdf_slice
	(
	text,
	integer,
	integer
	)
RETURNS text
LANGUAGE plshu
AS E'#!/bin/bash
PDFTOTEXT="/usr/bin/pdftotext -q -layout -f $2 -l $3 \"$1\" - 2> /dev/null"
ENCA="/usr/bin/enca -L __ -P -x UTF8 2> /dev/null"
LYNX="/usr/bin/lynx -stdin -dump -dont_wrap_pre"
# CMD="$PDFTOTEXT|$ENCA"
CMD="eval $ENCA < <( eval $PDFTOTEXT ) 2> /dev/null"
echo $(eval $CMD)
';