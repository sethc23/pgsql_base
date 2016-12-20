CREATE OR REPLACE FUNCTION z_file_metadata
	(
	fpath text
	)
RETURNS jsonb
LANGUAGE plpythonu
AS $function$ 
        import json
        import sys, yaml
        import xmltodict

        from subprocess                         import Popen as sub_popen
        from subprocess                         import PIPE as sub_PIPE

        def run_cmd(cmd):
            p=sub_popen(cmd,stdout=sub_PIPE,shell=True,executable='/bin/bash')
            (_out,_err) = p.communicate()
            assert _err is None
            return _out.rstrip('\n')

        
        f_ext = fpath[fpath.rfind('.')+1:]
        d1 = plpy.execute("select z_metadata_tika('"+fpath+"') res")[0]['res']
        try:
            d1 = eval(d1)
            d1 = d1[0]
        except:
            plpy.info('ERROR: z_file_metadata:d1:eval - fpath: '+fpath)
            
        if f_ext=='pdf':
            plpy.info(fpath)
            r = run_cmd("pdfinfo %s 2>/dev/null | sed 's/" % fpath)
            try:
                n = yaml.load(r)
            except:
                _r=''
                for it in r.split('\n'):
                    if it.count(':')>1:
                        pass
                    else:
                        _r+=it
                plpy.info(_r)
                try:
                    n = yaml.load(_r)
                except:
                    plpy.info('ERROR: z_file_metadata: fpath: '+fpath)
            if n:
                d1.update(n)

            r = run_cmd('|'.join(["pdfinfo -meta "+fpath+" 2>/dev/null","grep -E \^'[^a-z]'",]))
            mrk = 'Metadata:\n'
            
            
            if not r.find(mrk)==-1:
                _info = []
                pt = r.find(mrk)+len(mrk)
                r2 = r[pt:]
                
                try:
                    d2 = xmltodict.parse(r2, xml_attribs=False)
                    _info = d2["x:xmpmeta"]["rdf:RDF"]["rdf:Description"]
                except:
                    _r2=''
                    for it in r2.split('\n'):
                        if it.count(':')>1:
                            pass
                        else:
                            _r2+=it
                    plpy.info(_r2)
                    try:
                        d2 = xmltodict.parse(_r2, xml_attribs=False)
                        _info = d2["x:xmpmeta"]["rdf:RDF"]["rdf:Description"]
                    except:
                        plpy.info('ERROR: z_file_metadata: fpath: '+fpath)
                    
                

                for it in _info:
                    try:
                        d1.update(it)
                    except:
                        plpy.info(it)
        

        
        return json.dumps(d1,sort_keys=True)


    $function$;