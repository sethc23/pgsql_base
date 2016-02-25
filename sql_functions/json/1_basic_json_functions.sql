DROP FUNCTION IF EXISTS json_object_set_keys(json,text[],anyarray);
CREATE OR REPLACE FUNCTION json_object_set_keys(
  "json"          json,
  "keys_to_set"   TEXT[],
  "values_to_set" anyarray
)
  RETURNS json
  LANGUAGE sql
  IMMUTABLE
  STRICT
AS $function$
SELECT concat('{', string_agg(to_json("key") || ':' || "value", ','), '}')::json
FROM (SELECT *
      FROM json_each("json")
     WHERE "key" <> ALL ("keys_to_set")
     UNION ALL
    SELECT DISTINCT ON ("keys_to_set"["index"])
           "keys_to_set"["index"],
           CASE
             WHEN "values_to_set"["index"] IS NULL THEN 'null'::json
             ELSE to_json("values_to_set"["index"])
           END
      FROM generate_subscripts("keys_to_set", 1) AS "keys"("index")
      JOIN generate_subscripts("values_to_set", 1) AS "values"("index")
     USING ("index")) AS "fields"
$function$;

CREATE OR REPLACE FUNCTION json_append(data json, insert_data json)
RETURNS json
IMMUTABLE
LANGUAGE sql
AS $$
    SELECT ('{'||string_agg(to_json(key)||':'||value, ',')||'}')::json
    FROM (
        SELECT * FROM json_each(data)
        UNION ALL
        SELECT * FROM json_each(insert_data)
    ) t;
$$;
 
CREATE OR REPLACE FUNCTION json_delete(data json, keys text[])
RETURNS json
IMMUTABLE
LANGUAGE sql
AS $$
    SELECT ('{'||string_agg(to_json(key)||':'||value, ',')||'}')::json
    FROM (
        SELECT * FROM json_each(data)
        WHERE key <>ALL(keys)
    ) t;
$$;
 
CREATE OR REPLACE FUNCTION json_merge(data json, merge_data json)
RETURNS json
IMMUTABLE
LANGUAGE sql
AS $$
    SELECT ('{'||string_agg(to_json(key)||':'||value, ',')||'}')::json
    FROM (
        WITH to_merge AS (
            SELECT * FROM json_each(merge_data)
        )
        SELECT *
        FROM json_each(data)
        WHERE key NOT IN (SELECT key FROM to_merge)
        UNION ALL
        SELECT * FROM to_merge
    ) t;
$$;
 
CREATE OR REPLACE FUNCTION json_update(data json, update_data json)
RETURNS json
IMMUTABLE
LANGUAGE sql
AS $$
    SELECT ('{'||string_agg(to_json(key)||':'||value, ',')||'}')::json
    FROM (
        WITH old_data AS (
            SELECT * FROM json_each(data)
        ), to_update AS (
            SELECT * FROM json_each(update_data)
            WHERE key IN (SELECT key FROM old_data)
        )
    SELECT * FROM old_data
    WHERE key NOT IN (SELECT key FROM to_update)
    UNION ALL
    SELECT * FROM to_update
) t;
$$;

CREATE OR REPLACE FUNCTION json_lint(from_json json, ntab integer DEFAULT 0)
RETURNS json
LANGUAGE sql
IMMUTABLE STRICT
AS $$
SELECT (CASE substring(from_json::text FROM E'(?m)^[\s]*(.)') /* Get first non-whitespace */
        WHEN '[' THEN
                (E'[\n'
                        || (SELECT string_agg(repeat(E'\t', ntab + 1) || json_lint(value, ntab + 1)::text, E',\n') FROM json_array_elements(from_json)) ||
                E'\n' || repeat(E'\t', ntab) || ']')
        WHEN '{' THEN
                (E'{\n'
                        || (SELECT string_agg(repeat(E'\t', ntab + 1) || to_json(key)::text || ': ' || json_lint(value, ntab + 1)::text, E',\n') FROM json_each(from_json)) ||
                E'\n' || repeat(E'\t', ntab) || '}')
        ELSE
                from_json::text
END)::json
$$;

CREATE OR REPLACE FUNCTION json_unlint(from_json json)
RETURNS json
LANGUAGE sql
IMMUTABLE STRICT
AS $$
SELECT (CASE substring(from_json::text FROM E'(?m)^[\s]*(.)') /* Get first non-whitespace */
    WHEN '[' THEN
        ('['
            || (SELECT string_agg(json_unlint(value)::text, ',') FROM json_array_elements(from_json)) ||
        ']')
    WHEN '{' THEN
        ('{'
            || (SELECT string_agg(to_json(key)::text || ':' || json_unlint(value)::text, ',') FROM json_each(from_json)) ||
        '}')
    ELSE
        from_json::text
END)::json
$$;