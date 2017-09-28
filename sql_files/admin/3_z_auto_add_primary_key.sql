
DROP FUNCTION IF EXISTS public.z_auto_add_primary_key() CASCADE;
CREATE OR REPLACE FUNCTION public.z_auto_add_primary_key()
    RETURNS event_trigger AS
$BODY$
DECLARE
    has_index boolean;
    tbl_name text;
    primary_key_col text;
    missing_primary_key boolean;
    has_uid_col boolean;
    _seq text;
BEGIN
    select relhasindex,relname into has_index,tbl_name
        from pg_class
        where relnamespace=2200
        and relkind='r'
        order by oid desc limit 1;
    IF (
        pg_trigger_depth()=0
        and has_index=False )
    THEN
        --RAISE NOTICE 'NOT HAVE INDEX';
        EXECUTE format('SELECT a.attname
                        FROM   pg_index i
                        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                             AND a.attnum = ANY(i.indkey)
                        WHERE  i.indrelid = ''%s''::regclass
                        AND    i.indisprimary',tbl_name)
        INTO primary_key_col;

        missing_primary_key = (select primary_key_col is null);

        IF missing_primary_key=True
        THEN
            --RAISE NOTICE 'IS MISSING PRIMARY KEY';
            _seq = FORMAT('%I_uid_seq',tbl_name);
            EXECUTE FORMAT('SELECT count(*)!=0
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE table_name = ''%s''
                            AND column_name = ''uid''',tbl_name)
                INTO has_uid_col;

            IF (has_uid_col=True) THEN
                --RAISE NOTICE 'HAS UID COL';
                EXECUTE format('ALTER TABLE %I
                                    ALTER COLUMN uid TYPE INTEGER,
                                    ALTER COLUMN uid SET NOT NULL,
                                    ALTER COLUMN uid SET DEFAULT z_next_free(
                                        ''%s''::text,
                                        ''uid''::text,
                                        ''%s''::text),
                                    ADD PRIMARY KEY (uid);',tbl_name,tbl_name,_seq);
            ELSE
                --RAISE NOTICE 'NOT HAVE UID COL';
                _seq = FORMAT('%I_uid_seq',tbl_name);
                EXECUTE FORMAT('ALTER TABLE %I ADD COLUMN uid INTEGER PRIMARY KEY
                                DEFAULT z_next_free(''%s'',''uid'',''%s'');',
                                tbl_name,tbl_name,_seq);
            END IF;

        END IF;

    END IF;

END;
$BODY$
    LANGUAGE plpgsql;

--DROP EVENT TRIGGER IF EXISTS missing_primary_key_trigger;
CREATE EVENT TRIGGER missing_primary_key_trigger
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE','CREATE TABLE AS')
EXECUTE PROCEDURE z_auto_add_primary_key();