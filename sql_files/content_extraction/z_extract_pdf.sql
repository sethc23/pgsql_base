CREATE OR REPLACE FUNCTION z_extract_pdf
	(
	text
	)
RETURNS text
LANGUAGE plshu
AS $function$ 
#!/bin/bash
#/usr/bin/lynx --dump -nomargins -dont_wrap_pre <(/usr/bin/pdftotext -q -nopgbrk "$1" - | /usr/bin/enca -L __ -P -x UTF8)
res=$(/usr/bin/pdftotext -q -nopgbrk -layout "$1" - 2> /dev/null| /usr/bin/enca -L __ -P -x UTF8 | /usr/bin/lynx --stdin --dump -nomargins -dont_wrap_pre)
[[ -z $res ]] && \
    echo "$res" || \
/usr/bin/pdftotext -q -nopgbrk -layout "$1" - 2> /dev/null| /usr/bin/enca -L __ -P -x UTF8
$function$;