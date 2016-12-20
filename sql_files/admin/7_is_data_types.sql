CREATE OR REPLACE FUNCTION isdigit(
    double precision)
    RETURNS boolean
    LANGUAGE 'sql'
    AS $function$

    select $1::text ~ '^(-)?(0[.])?[0-9]+$' as result

    $function$;

CREATE OR REPLACE FUNCTION isdigit(
    text)
    RETURNS boolean
    LANGUAGE 'sql'
    AS $function$

    select $1::text ~ '^(-)?(0[.])?[0-9]+$' as result

    $function$;

CREATE OR REPLACE FUNCTION isnumeric(
    text)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    AS $function$

    DECLARE x NUMERIC;
    BEGIN
        x = $1::NUMERIC;
        RETURN TRUE;
    EXCEPTION WHEN others THEN
        RETURN FALSE;
    END;

    $function$;