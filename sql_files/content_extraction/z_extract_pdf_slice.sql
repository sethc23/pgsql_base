CREATE OR REPLACE FUNCTION z_extract_pdf_slice
	(
	text,
	integer,
	integer
	)
RETURNS text
LANGUAGE plshu
AS $function$ 
#!/bin/bash
#/usr/bin/lynx --dump -nomargins -dont_wrap_pre <(/usr/bin/pdftotext -q -nopgbrk "$1" - | /usr/bin/enca -L __ -P -x UTF8)
res=$(/usr/bin/pdftotext -q -layout -f $2 -l $3 "$1" - 2> /dev/null| /usr/bin/enca -L __ -P -x UTF8 | /usr/bin/lynx --stdin --dump -nomargins -dont_wrap_pre)
[[ -z $res ]] && \
    echo "$res" || \
/usr/bin/pdftotext -q -layout -f $2 -l $3 "$1" - 2> /dev/null| /usr/bin/enca -L __ -P -x UTF8
$function$;