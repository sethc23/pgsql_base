CREATE OR REPLACE FUNCTION z_extract_pdf_pages
	(
	fpath text
	)
RETURNS text[]
LANGUAGE plpythonu
AS $function$ 
c = '/usr/bin/pdfinfo %s|grep -i pages|sed "s/\D//g")' % fpath
pg_cnt = plpy.execute("select (select * from z_pdf_pgcnt('%s')) r" % fpath)[0]['r']
if pg_cnt:
    return [ plpy.execute("select z_extract_pdf_slice('%s',%d,%d) r" % (fpath,i,i))[0]['r'] for i in range(1,pg_cnt+1) ]
else:
    return None
$function$;