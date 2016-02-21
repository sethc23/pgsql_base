
-- Trigger Function: z_auto_add_last_updated_field()
DROP FUNCTION if exists z_auto_add_last_updated_field() cascade;
CREATE OR REPLACE FUNCTION z_auto_add_last_updated_field()
  RETURNS event_trigger AS
$BODY$
DECLARE
    last_table text;
    has_last_updated boolean;
BEGIN
    last_table := ( select relname from pg_class
                    where relnamespace=2200
                    and relkind='r'
                    order by oid desc limit 1);

    SELECT count(*)>0 INTO has_last_updated FROM information_schema.columns
        where table_name='||quote_ident(last_table)||'
        and column_name='last_updated';

    IF (
        pg_trigger_depth()=0
        and has_last_updated=False
        and position('tmp_' in last_table)=0 )
    THEN
        execute format('alter table %I drop column if exists last_updated',last_table);
        execute format('alter table %I add column last_updated timestamp with time zone',last_table);
        execute format('DROP FUNCTION if exists z_auto_update_timestamp_on_%s_in_last_updated() cascade',last_table);
        execute format('DROP TRIGGER if exists update_timestamp_on_%s_in_last_updated ON %s',last_table,last_table);

        execute format('CREATE OR REPLACE FUNCTION z_auto_update_timestamp_on_%s_in_last_updated()'
                        || ' RETURNS TRIGGER AS $$'
                        || ' BEGIN'
                        || '     NEW.last_updated := now();'
                        || '     RETURN NEW;'
                        || ' END;'
                        || ' $$ language ''plpgsql'';'
                        || '',last_table);

        execute format('CREATE TRIGGER update_timestamp_on_%s_in_last_updated'
                        || ' BEFORE UPDATE OR INSERT ON %I'
                        || ' FOR EACH ROW'
                        || ' EXECUTE PROCEDURE z_auto_update_timestamp_on_%s_in_last_updated();'
                        || '',last_table,last_table,last_table);

    END IF;

END;
$BODY$
  LANGUAGE plpgsql;

DROP EVENT TRIGGER if exists missing_last_updated_field;
CREATE EVENT TRIGGER missing_last_updated_field
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE','CREATE TABLE AS')
EXECUTE PROCEDURE z_auto_add_last_updated_field();