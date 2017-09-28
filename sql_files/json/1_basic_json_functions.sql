DROP FUNCTION IF EXISTS public.json_object_set_keys(json,text[],anyarray);

CREATE OR REPLACE FUNCTION public.json_object_update_key(
    json json,
    key_to_set text,
    value_to_set anyelement)
    RETURNS json
    LANGUAGE 'sql'
    AS $function$

        SELECT CASE
          WHEN ("json" -> "key_to_set") IS NULL THEN "json"
          ELSE (SELECT concat('{', string_agg(to_json("key") || ':' || "value", ','), '}')
                  FROM (SELECT *
                          FROM json_each("json")
                         WHERE "key" <> "key_to_set"
                         UNION ALL
                        SELECT "key_to_set", to_json("value_to_set")) AS "fields")::json
        END

    $function$;

CREATE OR REPLACE FUNCTION public.json_object_set_path(
    json JSON,
    key_path TEXT[],
    value_to_set anyelement)
    RETURNS JSON
    LANGUAGE 'sql'
    AS $function$

    SELECT CASE COALESCE(array_length("key_path", 1), 0)
             WHEN 0 THEN to_json("value_to_set")
             WHEN 1 THEN "json_object_set_key"("json", "key_path"[l], "value_to_set")
             ELSE "json_object_set_key"(
               "json",
               "key_path"[l],
               "json_object_set_path"(
                 COALESCE(NULLIF(("json" -> "key_path"[l])::TEXT, 'null'), '{}')::JSON,
                 "key_path"[l+1:u],
                 "value_to_set"
               )
             )
           END
      FROM array_lower("key_path", 1) l,
           array_upper("key_path", 1) u

    $function$;

CREATE OR REPLACE FUNCTION public.json_object_set_key(
    json json,
    key_to_set text,
    value_to_set anyelement)
    RETURNS json
    LANGUAGE 'sql'
    AS $function$

        SELECT concat('{', string_agg(to_json("key") || ':' || "value", ','), '}')::json
          FROM (SELECT *
                  FROM json_each("json")
                 WHERE "key" <> "key_to_set"
                 UNION ALL
                SELECT "key_to_set", to_json("value_to_set")) AS "fields"

    $function$;

CREATE OR REPLACE FUNCTION public.json_object_set_keys(
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

CREATE OR REPLACE FUNCTION public.json_append(data json, insert_data json)
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
COMMENT ON function public.json_append(json,json) IS
    '
        SELECT json_append(''{"a":"a res"}''::JSON,''{"b":"b res"}''::JSON)
        --> {"a": "NEW a res","b": "b res"}

        SELECT json_append(''{"a":"a res"}''::JSON,''{"a":"NEW a res"}''::JSON)
        --> {"a": "NEW a res"}
    ';

CREATE OR REPLACE FUNCTION public.json_delete(data json, keys text[])
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
COMMENT ON function public.json_delete(json,text[]) IS
    '
        SELECT json_delete(''{"a":"a res","b":"b res"}''::JSON,''{"b"}'')
        --> {"a": "a res"}

    ';

CREATE OR REPLACE FUNCTION public.json_merge(data json, merge_data json)
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
COMMENT ON function public.json_merge(json,json) IS
    '
        SELECT json_merge(''{"a":"a res"}''::JSON,''{"b":"b res"}''::JSON)
        --> {"a": "NEW a res","b": "b res"}

        SELECT json_merge(''{"a":"a res"}''::JSON,''{"a":"NEW a res"}''::JSON)
        --> {"a": "NEW a res"}
    ';

CREATE OR REPLACE FUNCTION public.json_update(data json, update_data json)
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
COMMENT ON function public.json_update(json,json) IS
    '
        SELECT json_update(''{"a":"a res"}''::JSON,''{"a":"NEW a res"}''::JSON)
        --> {''a'': ''NEW a res''}
    ';

CREATE OR REPLACE FUNCTION public.json_lint(from_json json, ntab integer DEFAULT 0)
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

CREATE OR REPLACE FUNCTION public.json_unlint(from_json json)
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

CREATE OR REPLACE FUNCTION public.z_json_to_table
    (
    _idx INTEGER,
    _json JSON
    )
    RETURNS SETOF TEXT
    LANGUAGE PLPGSQL
    AS $FUNCTION$
    BEGIN
        RETURN QUERY
        SELECT
            _i
            ,_key
            ,(_j->_key)::JSON _val
        FROM (
            SELECT
                i _i,
                j _j,
                json_object_keys(j::jsOn) _key
            FROM (
                select
                    _idx i,
                    _json::JSON j
            ) f1
        ) f2;
    END
    $FUNCTION$;

