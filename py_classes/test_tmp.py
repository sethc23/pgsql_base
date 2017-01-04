import inspect as I
with open(__file__,'r') as f:
    this_file                               =   f.read().split('\n')
from example import Start

x                                           =   Start()

# Show Existing Variables
print '\nVariables Defined in dictionary ("D")'
print '\n\t',x.T.guid,'\tguid'
print '\t',x.T.user,'\tuser (from environment variable \'USER\')\n'

print '\n\nUsage Examples ...'

# Show how to redefine variables
old_guid = x.T.guid
x.T.guid = str(x.T.get_guid().hex)[:7]
print '\nRedefine Variable:\n\t%s' % this_file[ I.stack()[0][2] - 2 ]
assert x.T.guid != old_guid
print '\tprint x.T.guid \n-->\t%s' % x.T.guid


# Show how to add variables to object shared across all objects
assert not hasattr(x.T,'guid_2')
x.T.guid_2 = str(x.T.get_guid().hex)[:7]
print '\nAdd Variable to Shared Object:\n\t%s' % this_file[ I.stack()[0][2] - 2 ]
assert hasattr(x.T,'guid_2')
print '\tprint x.T.guid_2 \n-->\t%s' % x.T.guid_2


# Distinguish between adding dictionary as variable versus integrating dictionary with shared objects

#   -- adding dictionary
d = {'one':1,'two':2}
print '\nAdd Dictionary to Shared Object:\n\t%s' % this_file[ I.stack()[0][2] - 2 ]
assert not hasattr(x.T,'new_dictionary')
x.T.new_dictionary = d
print '\t%s' % this_file[ I.stack()[0][2] - 2 ]
assert hasattr(x.T,'new_dictionary')
print '\tprint x.T.new_dictionary \n-->\t%s' % x.T.new_dictionary

#   -- integrating dictionary
d = {'one':1,'two':2}
print '\nIntegrate Dictionary with Shared Object:\n\t%s' % this_file[ I.stack()[0][2] - 2 ]
assert not hasattr(x.T,'one') and not hasattr(x.T,'two')
x.T.update(d)
print '\t%s' % this_file[ I.stack()[0][2] - 2 ]
assert hasattr(x.T,'one') and hasattr(x.T,'two')
assert x.T.one + x.T.two == 3
print '\tprint x.T.one + x.T.two \n-->\t%s' % (x.T.one + x.T.two)


print '\n'

print '\nIllustration of cross-sub-class accessibility...\n(refer to example.py if unclear)\n'

from example import Start
x = Start()
x.One.Alpha.funct_1()
x.Two.Bravo.funct_1()