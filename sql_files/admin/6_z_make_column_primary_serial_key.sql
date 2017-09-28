

-- DROP FUNCTION IF EXISTS public.z_make_column_primary_serial_key(text,text,boolean,boolean,boolean);
CREATE OR REPLACE FUNCTION public.z_make_column_primary_serial_key(
    IN tbl text,
    IN uid_col text,
    IN is_new_col boolean,
    IN is_primary_key boolean,
    IN has_default boolean)
RETURNS VOID AS
$$
DECLARE
    _seq text;
BEGIN

    IF (is_new_col=True) THEN
        _seq = FORMAT('%I_%s',tbl,uid_col);
        EXECUTE FORMAT('ALTER TABLE %I ADD COLUMN %s INTEGER PRIMARY KEY DEFAULT z_next_free(
                            ''%s''::text, ''uid''::text, ''%s''::text);',
                                tbl,uid_col,
                                tbl,_seq);

    ELSE

        IF (is_primary_key=False) THEN
            EXECUTE FORMAT('alter table %I add primary key(%s);',tbl,uid_col);
        END IF;

        IF (has_default=False) THEN
            _seq = FORMAT('%I_%s',tbl,uid_col);
            EXECUTE FORMAT('alter table %I alter column %s set default z_next_free(
                            ''%s''::text, ''uid''::text, ''%s''::text);',
                                tbl,uid_col,
                                tbl,_seq);
        END IF;


    END IF;

            --UPDATE %(tbl)s SET %(uid_col)s =
            --    nextval(pg_get_serial_sequence('%(tbl)s','%(uid_col)s'));

    --IF (is_primary_key=False)
    --THEN
    --END IF;

    --execute format('alter table %I alter column %s add primary key (%s);',tbl,uid_col);
    --_seq = format('%I_%s',tbl,uid_col);
    --execute format('alter table %I alter column %s set default z_next_free(''%s'',''%s'',''%s'')',
    --                           tbl,            uid_col,                    tbl,uid_col,_seq);
    --execute format('alter table %I alter column %s set default
    --                    nextval(pg_get_serial_sequence(''%I'',''%s''));',
    --                ,tbl,uid_col,tbl,uid_col);

END;
$$
LANGUAGE plpgsql;

COMMENT ON FUNCTION public.z_make_column_primary_serial_key(text,text,boolean,boolean,boolean) IS '
    Usage:
                    select z_make_column_primary_serial_key({table_name}::text,
                                                            {col_name}::text,
                                                            {is_new_col}::boolean,
                                                            {is_primary_key}::boolean,
                                                            {has_default}::boolean)
';