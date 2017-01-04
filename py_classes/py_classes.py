
def To_Sub_Classes(_self,_parent):
    _self._parent                           =   _parent

    # Create branch for current object
    tmp,branch,depth                        =   _self,[],0
    while hasattr(tmp,'_parent'):
        branch.append(                          tmp.__class__.__name__)
        tmp                                 =   tmp._parent
        depth                              +=  1
    branch.append(                              tmp.__class__.__name__)
    branch.reverse()
    orig_branch_names                       =   branch[:]
    branch                                  =   [branch[0]] + [it.replace(branch[0]+'_','') for it in branch[1:]]
    # print branch
    _self.C,_self.F,_self.T                 =   _parent.C,_parent.F,_parent.T

    '''
    RE: following conditional:
        Action:     where BASE_OBJECT inherits sub-class at top-most level, create an object to hold all nested sub-sub-class(es).
        Purpose:    maintain base sub-class access point that is available in any nested function/class of any level

    '''
    if not hasattr(_self,branch[1]) and not _self.__class__.__name__==orig_branch_names[1]:
        setattr(                                _self,branch[1],To_Class())

    method_to_base_str                      =   '_'.join( [ it for it in branch[1:] ])
    for it in dir(_self):
        _current                            =   getattr(_self,it)
        if type(_current).__name__=='classobj':

            # where first-level sub-class, initiate objects in sub-class
            if _self.__class__.__name__==orig_branch_names[1]:
                setattr(                        _self,it,_current(_self))

            # where _self inherits sub-class, add sub-class to branch
            else:
                setattr(                        getattr(_self,branch[1]),
                                                it,_current(_self))
            _self.C.update(                 { '%s_%s' % (method_to_base_str,it)   :   _current(_self) } )

        elif _current.__class__.__name__=='instancemethod' and not ['_','T'].count(it[0]):
            _self.F.update(                     { '%s_%s' % (method_to_base_str.lower(),it.lower())   :   _current } )

    globals().update(                           _self.T.__dict__)
    return _self

def To_Class_Dict(_self,dict_list=[],update_globals=True):
    all_dicts                               =   {}
    for it in dict_list:
        all_dicts.update(                       it)

    class_objs                              =   ['C','F','T']
    # re: class_objs -- F: functions, T: everything else including imported modules
    for it in class_objs:
        if not hasattr(_self,it):
            setattr(                            _self,it,To_Class())

    for k,v in all_dicts.iteritems():
        if not k=='self':
            _self.T.update(                      {k                      :   v })

    globals().update(                           _self.T.__dict__)
    return _self.T

class To_Class:
    """
    Although a dictionary may seem easier to setup,
    object classes are easier to use.

    Dictionary:     D['some_item']
        vs.
    Object Class:   D.some_item
    """
    def __init__(self, init=None,recursive=False):
        if init is not None:
            self.__dict__.update(init)
        if recursive:
            for k,v in self.__dict__.items():
                if type(v)==dict:
                    setattr(self,k,To_Class(v))

    def __allitems__(self):
        return self.__dict__.keys()

    def __dict__(self):
        return dict(zip(self.__dict__.keys(),self.__dict__.values()))

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value
        # if type(value)==dict:
        #     for k,v in value.iteritems():
        #         self.__dict__[k] = v

    def __delitem__(self, key):
        del self.__dict__[key]

    def __contains__(self, key):
        return key in self.__dict__

    def __len__(self):
        return len(self.__dict__)

    def __repr__(self):
        return repr(self.__dict__)
    
    def _has_key(self,key):
        return self.__dict__.has_key(key)

    def _update(self,upd):
        return self.__init__(upd)

    def update(self,upd):
        return self.__init__(upd)


