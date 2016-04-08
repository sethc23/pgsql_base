
CREATE EXTENSION IF NOT EXISTS pllua;

-- DROP TYPE IF EXISTS str_match_res CASCADE;
CREATE TYPE z_str_match_res AS (
    a_idx TEXT,
    a_str TEXT,
    jaro_score TEXT,
    b_str TEXT,
    b_idx TEXT,
    other_matches TEXT
);

-- DROP FUNCTION IF EXISTS         z_str_matching(  text,text,text  );
CREATE OR REPLACE FUNCTION      z_str_matching(  qry_a text, qry_b text, with_permutations text  )

RETURNS SETOF z_str_match_res AS $BODY$

    package.loaded.string_matching = nil
    str_m = require "string_matching"
    str_m.iter_jaro(qry_a,qry_b,with_permutations)


$BODY$ LANGUAGE plluau;

COMMENT ON FUNCTION z_str_matching(  text,text,text  ) IS E'
    The input querys A and B must provide the aliases (a_str, a_idx),(b_str, b_idx), respectively.
    For each a_str, this function finds the best matching b_str,
        and returns (a_str, a_idx, jaro_score, b_str, b_idx, other_matching).
    "other_matching" provides the b_idx for other b_str having the same highest score.
    Avoid using "pllua_" as an a prefix for any alias within either qry_a or qry_b.

    "with_permutations" will make this function further consider all permutations of b_str
        as split by any of the dividing character segment(s).
        To consider permutations of b_str "one_two_three" (e.g., "two_one_three", etc...)
            provide "_" or "_;" as the value for "with_permutations".
        A ";" marks a break point between split characters.
        For a single space, " ;" must be at beginning if at all.
        For a single emi-colon, ";;" must be at the end if at all.
        Default value for "with_permutations" is " ;-;_;/;\\;|;&;;""'