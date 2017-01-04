
CREATE OR REPLACE FUNCTION x64_decode(enc_str TEXT)
RETURNS SETOF TEXT AS $BODY$
BEGIN
    RETURN QUERY SELECT convert_from(decode(enc_str, 'base64'), 'utf-8')::TEXT;
END
$BODY$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION x64_decode(enc_str BYTEA)
RETURNS SETOF TEXT AS $BODY$
BEGIN
    RETURN QUERY SELECT convert_from(enc_str, 'utf-8')::TEXT;
END
$BODY$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION x64_encode(raw_str TEXT)
RETURNS SETOF TEXT AS $BODY$
BEGIN
    RETURN QUERY SELECT encode(convert_to(raw_str, 'utf-8'), 'base64')::TEXT;
END
$BODY$ LANGUAGE PLPGSQL;