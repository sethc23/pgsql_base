
-- Trigger Function: z_auto_add_primary_key()
DROP FUNCTION IF EXISTS z_auto_add_primary_key() cascade;
CREATE OR REPLACE FUNCTION z_auto_add_primary_key()
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
            _seq = format('%I_uid_seq',tbl_name);
            EXECUTE format('select count(*)!=0 
                        from INFORMATION_SCHEMA.COLUMNS 
                        where table_name = ''%s''
                        and column_name = ''uid''',tbl_name)
            INTO has_uid_col;            
            IF (has_uid_col=True)
            THEN
                --RAISE NOTICE 'HAS UID COL';
                execute format('alter table %I 
                                    alter column uid type integer,
                                    alter column uid set not null,
                                    alter column uid set default z_next_free(
                                        ''%I'',
                                        ''uid'',
                                        ''%I''), 
                                    ADD PRIMARY KEY (uid);',tbl_name,tbl_name,_seq);
            ELSE
                --RAISE NOTICE 'NOT HAVE UID COL';
                _seq = format('%I_uid_seq',tbl_name);
                execute format('alter table %I add column uid integer primary key
                                default z_next_free(''%I'',''uid'',''%I'')',
                                tbl_name,tbl_name,_seq);
            END IF;
            
        END IF;

    END IF;
    
END;
$BODY$
  LANGUAGE plpgsql;

DROP EVENT TRIGGER if exists missing_primary_key_trigger;
CREATE EVENT TRIGGER missing_primary_key_trigger
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE','CREATE TABLE AS')
EXECUTE PROCEDURE z_auto_add_primary_key();