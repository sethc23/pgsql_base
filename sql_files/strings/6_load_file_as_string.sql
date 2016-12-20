CREATE FUNCTION file.read(file text)  
    RETURNS text AS $$
    DECLARE
        content text;
        tmp text;
    BEGIN
        file := quote_literal(file);
        tmp := quote_ident(uuid_generate_v4()::text);

        EXECUTE 'CREATE TEMP TABLE ' || tmp || ' (content text)';
        EXECUTE 'COPY ' || tmp || ' FROM ' || file;
        EXECUTE 'SELECT content FROM ' || tmp INTO content;
        EXECUTE 'DROP TABLE ' || tmp;

        RETURN content;
    END;
    $$ LANGUAGE plpgsql VOLATILE;

