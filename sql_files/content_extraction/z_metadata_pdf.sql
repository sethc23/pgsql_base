CREATE OR REPLACE FUNCTION z_metadata_pdf
	(
	fpath text
	)
RETURNS jsonb
LANGUAGE plpythonu
AS $function$ 

        import sys, yaml, json
        import xmltodict

        from subprocess                         import Popen as sub_popen
        from subprocess                         import PIPE as sub_PIPE

        def run_cmd(cmd):
            p=sub_popen(cmd,stdout=sub_PIPE,shell=True,executable='/bin/bash')
            (_out,_err) = p.communicate()
            assert _err is None
            return _out.rstrip('\n')

        r = run_cmd('pdfinfo %s 2>/dev/null' % fpath)
        d1 = yaml.load(r)

        r = run_cmd('|'.join(["pdfinfo -meta "+fpath+" 2>/dev/null","grep -E \^'[^a-z]'",]))
        mrk = 'Metadata:\n'
        pt = r.find(mrk)+len(mrk)

        d2 = xmltodict.parse(r[pt:], xml_attribs=False)
        _info = d2["x:xmpmeta"]["rdf:RDF"]["rdf:Description"]

        for it in _info:
            d1.update(it)
        #return json.dumps(d1, indent=4, separators=(',', ': '),sort_keys=True)
        return json.dumps(d1,sort_keys=True)

    $function$;