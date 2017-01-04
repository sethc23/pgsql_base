class Two:
    def __init__(self,_parent):
        self                                =   _parent.T.To_Sub_Classes(self,_parent)
    class Bravo:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)
        def funct_1(self):
            print                               'Called funct_1 of class "Bravo".'

            method_1                        =   'self.F.one_alpha_funct_1()'
            print                               '\n--> calling: %s' % method_1
            res_1                           =   eval(method_1)
            print                               '<-- call finished'

            method_2                        =   'self.C.One_Alpha.funct_1()'
            print                               '\n--> calling: %s' % method_2
            res_2                           =   eval(method_2)
            print                               '<-- call finished'

            assert res_1 == res_2

            print '\n'

class One:
    def __init__(self,_parent):
        self                                =   _parent.T.To_Sub_Classes(self,_parent)
    class Alpha:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)
        def funct_1(self):
            print                               'Called funct_1 of class "Alpha".'


class Start:
    def __init__(self):
        from py_classes                         import To_Class_Dict,To_Sub_Classes
        from os                                 import environ          as os_environ
        from uuid                               import uuid4            as get_guid
        D                                   =   {'guid'                 :   str(get_guid().hex)[:7],
                                                 'user'                 :   os_environ['USER'],
                                                }

        self.T                              =   To_Class_Dict(  self,
                                                                dict_list=[ D, locals() ],
                                                                update_globals=True
                                                              )
        self.One                            =   One(self)
        self.Two                            =   Two(self)
