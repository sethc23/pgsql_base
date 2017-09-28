
CREATE EXTENSION IF NOT EXISTS pllua;

-- DROP FUNCTION IF EXISTS         public.z_str_comp_lcs( text,text );
CREATE OR REPLACE FUNCTION      public.z_str_comp_lcs( s1              text,
                                                s2              text)
RETURNS                         text
AS $BODY$


function LCS ( a, b )
    if #a == 0 or #b == 0 then
        return                  ""
    elseif a:sub( -1, -1 ) == b:sub( -1, -1 ) then
        return                  LCS( a:sub( 1, -2 ), b:sub( 1, -2 ) ) .. a:sub( -1, -1 )
    else
        local a_sub         =   LCS( a, b:sub( 1, -2 ) )
        local b_sub         =   LCS( a:sub( 1, -2 ), b )

        if #a_sub > #b_sub then
            return              a_sub
        else
            return              b_sub
        end
    end
end

local res                   =   LCS(s1,s2)
return res

$BODY$ LANGUAGE plluau;

COMMENT ON FUNCTION public.z_str_comp_lcs( text,text ) IS 'Longest Common Subsequence';