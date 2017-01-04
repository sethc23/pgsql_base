
### PROBLEM:

Some might experience a re-occuring pattern in software project development, such as:

- sparse lines of codes become functions,
- functions become classes,
- classes become modules/files,
- files/directories become libraries,
- etc \.\.\.

(hereinafter "*Project Maturation*").

In addition to the substantive challenges raised by the particularities of a project, *Project Maturation* begets its own type of complexity in a number of ways, some of which currently include:

- interoperability;
- function/class/module inter-dependencies, developments and unit-testing; and
- code clarity, re-usability and community utility.

### SOLUTION PROPOSED:

A method for developing and maintaining a project that:

1. easily integrates into existing projects;
2. provides, at each system level, access points to reach any other system level ("*Access Points*");
3. uses a common syntax for connecting with *Access Points*; and
4. increases code execution efficiency in most cases or otherwise imposes negligible overhead.

### CURRENT IMPLEMENTATION:

Where a module has classes configured similar to **[this Gist](http://gist.github.com/sethc23/9a0020d2f6eb736d07e1)**, the proposed solution allows the following:

``` python
from example import Start
x = Start()

print 'Variables Defined in dictionary ("D")'
print '\t',x.T.guid,'first guid'
print '\t',x.T.user,'user defined by environmental variable'

# Redefine Objects
old_guid = x.T.guid
x.T.guid = str(x.T.get_guid().hex)[:7]
assert x.T.guid != old_guid

# Add New Objects
assert not hasattr(x.T,'guid_2')
x.T.guid_2 = str(x.T.get_guid().hex)[:7]
assert hasattr(x.T,'guid_2')

# Dictionaries Added as Objects by Default
d = {'one':1,'two':2}
assert not hasattr(x.T,'new_dictionary')
x.T.new_dictionary = d
assert hasattr(x.T,'new_dictionary')

# Integrate Dictionaries
d = {'one':1,'two':2}
assert not hasattr(x.T,'one') and not hasattr(x.T,'two')
x.T.update(d)
assert hasattr(x.T,'one') and hasattr(x.T,'two')
assert x.T.one + x.T.two == 3

```

Running the following illustrates methods for accessing components of other objects:
``` python
from example import Start
x = Start()
x.One.Alpha.funct_1()
x.Two.Bravo.funct_1()
```



