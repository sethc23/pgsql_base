
CREATE EXTENSION IF NOT EXISTS plpythonu;

-- Type: public.string_dist_results

DROP TYPE public.z_str_dist_results CASCADE;
CREATE TYPE public.z_str_dist_results AS
   (idx integer,
    orig_str text,
    jaro double precision,
    jaro_b text,
    leven integer,
    leven_b text,
    nysiis text,
    rating_codex text);



DROP FUNCTION IF EXISTS     public.z_str_get_dist(      integer[],
                                                    text,
                                                    text,
                                                    text[],
                                                    boolean,
                                                    boolean,
                                                    boolean,
                                                    boolean,
                                                    boolean) CASCADE;

CREATE OR REPLACE FUNCTION  public.z_str_get_dist(      idx             integer[],
                                                    string_set      text[],
                                                    compare_tbl     text,
                                                    compare_col     text[],
                                                    jaro            boolean default true,
                                                    leven           boolean default true,
                                                    nysiis          boolean default true,
                                                    rating_codex    boolean default true,
                                                    usps_repl_first boolean default true)
RETURNS SETOF public.z_str_dist_results AS $$

    from jellyfish              import cjellyfish as J
    from traceback              import format_exc       as tb_format_exc
    from sys                    import exc_info         as sys_exc_info

    class z_str_dist_results:

        def __init__(self,upd=None):
            if upd:
                self.__dict__.update(upd)


    important_cols          =   [   'street_name','from_street_name',
                                    'variation','primary_name',
                                    'common_use','usps_abbr','pattern']

    T                       =   {   'tbl'           :   compare_tbl,
                                    'concat_col'    :   ''.join(["concat_ws(' ',",
                                                                 ",".join(compare_col),
                                                                 ")"]),
                                    'not_null_cols' :   'WHERE ' + ' is not null and '.join([it for it in compare_col
                                                                            if important_cols.count(it)>0]) + ' is not null',
                                                                 }


    if T['not_null_cols']=='WHERE  is not null':
        T['not_null_cols']  =   ''

    #plpy.log(T)
    try:

        p                   =   "select distinct %(concat_col)s comparison from %(tbl)s %(not_null_cols)s;" % T
        res                 =   plpy.execute(p)
        if len(res)==0:
            plpy.log(           "string_dist_results: NO DATA AVAILABLE FROM %(tbl)s IN %(tbl)s" % T)
            return
        else:
            # plpy.log(res)
            res             =   map(lambda s: unicode(s['comparison']),res)

        #plpy.log("about to start")
        for i in range(len(idx)):
            #plpy.log("started")
            _word           =   unicode(string_set[i].upper())
            if not _word:
                plpy.log(       string_set)
                plpy.log(       "not word")
                plpy.log(       _word)
                yield(          None)

            else:

                t           =   {   'idx'           :   idx[i],
                                    'orig_str'      :   _word   }
                if jaro:
                    # plpy.log(t)
                    t.update(   dict(zip(['jaro','jaro_b'],
                                     sorted(map(lambda s: (J.jaro_distance(_word,s),s),res ) )[-1:][0])))
                if leven:
                    t.update(   dict(zip(['leven','leven_b'],
                                     sorted(map(lambda s: (J.levenshtein_distance(_word,s),s),res ) )[0:][0])))
                if nysiis:
                    t.update(   {   'nysiis'            :   J.nysiis(_word)                 })

                if rating_codex:
                    t.update(   {   'rating_codex'      :   J.match_rating_codex(_word)     })

                # plpy.log(t)
                r           =   string_dist_results(t)
                yield(          r)

        return

    except Exception as e:
        plpy.log(               tb_format_exc())
        plpy.log(               sys_exc_info()[0])
        plpy.log(               e)
        return

$$ LANGUAGE plpythonu;