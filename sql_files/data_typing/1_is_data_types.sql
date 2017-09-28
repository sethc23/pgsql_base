CREATE OR REPLACE FUNCTION public.isdigit(
    DOUBLE PRECISION)
    RETURNS BOOLEAN
    LANGUAGE 'sql'
    AS $FUNCTION$

    SELECT $1::TEXT ~ '^(-)?(0[.])?[0-9]+$' AS result

    $FUNCTION$;

CREATE OR REPLACE FUNCTION public.isdigit(
    TEXT)
    RETURNS BOOLEAN
    LANGUAGE 'sql'
    AS $FUNCTION$

    SELECT $1::TEXT ~ '^(-)?(0[.])?[0-9]+$' AS result

    $FUNCTION$;

CREATE OR REPLACE FUNCTION public.isnumeric(
    TEXT)
    RETURNS BOOLEAN
    LANGUAGE 'plpgsql'
    AS $FUNCTION$

    DECLARE x NUMERIC;
    BEGIN
        x = $1::NUMERIC;
        RETURN TRUE;
    EXCEPTION WHEN others THEN
        RETURN FALSE;
    END;

    $FUNCTION$;