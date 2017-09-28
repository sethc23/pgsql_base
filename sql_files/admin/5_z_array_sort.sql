
-- DROP FUNCTION IF EXISTS public.z_array_sort(anyarray);
CREATE OR REPLACE FUNCTION public.z_array_sort(anyarray)
    RETURNS anyarray AS
$BODY$
SELECT ARRAY(SELECT unnest($1) ORDER BY 1)
$BODY$
  LANGUAGE sql;