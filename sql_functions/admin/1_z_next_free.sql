CREATE EXTENSION IF NOT EXISTS plpythonu;

-- Function: z_next_free(text, text, text)
DROP FUNCTION IF EXISTS z_next_free(text, text, text) cascade;
CREATE OR REPLACE FUNCTION z_next_free(
    table_name text,
    uid_col text,
    _seq text)
  RETURNS integer AS
$BODY$
stop=False
T = {'tbl':table_name,'uid_col':uid_col,'_seq':_seq}
p = """

            select count(column_name) c
            from INFORMATION_SCHEMA.COLUMNS
            where table_name = '%(tbl)s'
            and column_name = '%(uid_col)s';

    """ % T
cnt = plpy.execute(p)[0]['c']

if cnt==0:
    p = "create sequence %(tbl)s_%(uid_col)s_seq start with 1;"%T
    t = plpy.execute(p)
    p = "alter table %(tbl)s alter column %(uid_col)s set DEFAULT z_next_free('%(tbl)s'::text, 'uid'::text, '%(tbl)s_uid_seq'::text);"%T
    t = plpy.execute(p)
stop=False
while stop==False:
    p = "SELECT nextval('%(tbl)s_%(uid_col)s_seq') next_val"%T
    try:
        t = plpy.execute(p)[0]['next_val']
    except plpy.spiexceptions.UndefinedTable:
        p = "select max(%(uid_col)s) from %(tbl)s;" % T
        max_num = plpy.execute(p)[0]['max']
        if max_num:
            T.update({'max_num':str(max_num)})
        else:
            T.update({'max_num':str(1)})
        p = "create sequence %(tbl)s_%(uid_col)s_seq start with %(max_num)s;" % T
        t = plpy.execute(p)
        p = "SELECT nextval('%(tbl)s_%(uid_col)s_seq') next_val"%T
        t = plpy.execute(p)[0]['next_val']
    T.update({'next_val':t})
    p = "SELECT count(%(uid_col)s) cnt from %(tbl)s where %(uid_col)s=%(next_val)s"%T
    chk = plpy.execute(p)[0]['cnt']
    if chk==0:
        stop=True
        break
return T['next_val']

$BODY$
LANGUAGE plpythonu;