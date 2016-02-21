
CREATE EXTENSION IF NOT EXISTS plpythonu;

-- Type: public.string_dist_results
DROP TYPE IF EXISTS string_dist_results CASCADE;
CREATE TYPE string_dist_results AS
   (idx integer,
    orig_str text,
    jaro double precision,
    jaro_b text,
    leven integer,
    leven_b text,
    nysiis text,
    rating_codex text);


DROP FUNCTION IF EXISTS     z_jellyfish(            text,
                                                    text,
                                                    boolean,
                                                    boolean,
                                                    boolean,
                                                    boolean,
                                                    boolean,
                                                    boolean,
                                                    boolean) CASCADE;

CREATE OR REPLACE FUNCTION  z_jellyfish(            from_str_idx_tuples_qry     text,                  -- having header: | from_tuples    |      
                                                    against_str_idx_tuples_qry  text,                  -- having header: | against_tuples |
                                                    all_results                 boolean default false,
                                                    best_result                 boolean default true,
                                                    jaro                        boolean default true,
                                                    leven                       boolean default true,
                                                    nysiis                      boolean default true,
                                                    rating_codex                boolean default true,
                                                    usps_repl_first             boolean default true)

RETURNS SETOF string_dist_results AS $$

    from jellyfish              import cjellyfish as J
    from traceback              import format_exc       as tb_format_exc
    from sys                    import exc_info         as sys_exc_info

    class string_dist_results:

        def __init__(self,upd=None):
            if upd:
                self.__dict__.update(upd)


    T                       =   {   'from_qry'              :   from_str_idx_tuples_qry,
                                    'against_qry'           :   against_str_idx_tuples_qry,
                                }
    plpy.log(                   T)

    try:

        p                   =   """
                                SELECT from_tuples,against_tuples
                                FROM
                                    (%(from_qry)s) f1,
                                    (%(against_qry)s) f2
                                """
        plpy.log(               p)
        res                 =   plpy.execute(p % T)
        if len(res)==0:
            plpy.log(           "string_dist_results: NO DATA AVAILABLE FROM ##(tbl)s IN %(tbl)s" % T)
            return
        else:
            plpy.log(           res)
            return
            res             =   map(lambda s: unicode(s['comparison']),res)

        for i in range(len(idx)):
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


COMMENT ON FUNCTION z_jellyfish( text,text,boolean,boolean,boolean,boolean,boolean,boolean,boolean ) IS E'

    compare_col is concat_ws('' '',...)

    General Idea:

        Given a list of tuples comprising strings and corresponding index values ("from_tuples"), and
        Given another list of tuples comprising strings and corresponding index values ("against_tuples");

        for _string,_idx in from_tuple.iteritems():
            find closest string match between _string and [ all strings in against_tuples ]


    Comments:

        input queries must use double single quotes ('''') in place of normal single quotes ('') used to indicate text types.

    Usage:

        Given:
            qry_1 = "select array[(1::integer,regexp_replace(''part_a-part_b'',''^([^-]*)-(.*)$'',''\\2'',''g''))] from_tuples"
            qry_2 = "select array[(101::integer,''no match here''),
                                  (102::integer,''partial match -part_b''),
                                  (103::integer,''part_b''),] against_tuples"

        Query:

            select z_jellyfish(qry_1,qry_2)

        Produces Results with Header:

            | from_str | from_idx | against_str | against_idx | jaro_b | etc ...

    '