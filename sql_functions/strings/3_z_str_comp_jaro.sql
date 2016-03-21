
CREATE EXTENSION IF NOT EXISTS pllua;

-- DROP FUNCTION IF EXISTS         z_str_comp_jaro(text,text,boolean,boolean,boolean,boolean);
CREATE OR REPLACE FUNCTION      z_str_comp_jaro(s1              text,
                                                s2              text,
                                                winklerize      boolean DEFAULT true,
                                                long_tolerance  boolean DEFAULT true,
                                                verbose         boolean DEFAULT false,
                                                reload_module   boolean DEFAULT false)
RETURNS                         double precision
AS $BODY$

    local t                     =   ""
    if reload_module then package.loaded.string_matching = nil end
    local str_m                 =   require "string_matching"
    t                           =   str_m.jaro_score(s1,s2,winklerize,long_tolerance,verbose)

    return t


$BODY$ 
LANGUAGE plluau;