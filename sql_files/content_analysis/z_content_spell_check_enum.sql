CREATE OR REPLACE FUNCTION z_content_spell_check_enum
	(
	content text[]
	)
RETURNS spell_score
LANGUAGE plpythonu
AS $function$ 
import                                  re
import                                  enchant as ENCH
SPELL_DICT                      =       ENCH.Dict("en_US")
import                                  pandas as pd
res_mean,res_std = [],[]
min_words_per_line = 8
for i in range(len(content)):
    txt_cons = re.sub(r"(\n[ \n]*)","\n",re.sub(r"( [ ]+)"," ",re.sub(r"(?i)([^a-z\n])"," ",content[i]))).strip(" \n")
    l_spell_chk = []
    for l in txt_cons.split("\n"):
        words = [ w for w in l.split(" ") if w]
        if len(words) >= min_words_per_line:
            correct = [ w for w in words if SPELL_DICT.check(w) ]
            l_spell_chk.append( float(len(correct)) / float(len(words)) )
    df = pd.DataFrame({"line_score":l_spell_chk})
    res_mean.append( df.line_score.mean() )
    res_std.append( df.line_score.std() )
return res_mean,res_std
$function$;