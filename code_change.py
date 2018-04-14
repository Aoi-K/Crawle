def code_change(s):
    if s==None:
        return ''
    s=s.replace(u'\u017a','z').replace(u'\u017b','Z').replace(u'\u017c','z').replace(u'\u017d','Z').replace(u'\u017e','z')\
            .replace(u'\u0151','y').replace(u'\xfd','y').replace(u'\xff','y') \
            .replace(u'\x9c', 'U').replace(u'\xda','U').replace(u'\xbc','u').replace(u'\xf9','u').replace(u'\u1ee7','u').replace(u'\u0169','u').replace(u'\u016b','u').replace(u'\xfc','u').replace(u'\u016f','u').replace(u'\xfa','u').replace(u'\xdc','U')\
            .replace(u'\u0141','t').replace(u'\u0163','t').replace(u'\u021b','t').replace(u'\u0165','t')\
            .replace(u'\u0160','S').replace(u'\u0161','s').replace(u'\u015a','S').replace(u'\u015b','s').replace(u'\u015f','s').replace(u'\u0219','s')\
            .replace(u'\u0159','r')\
            .replace(u'\xd8','O').replace(u'\xf2','o').replace(u'\xf3','o').replace(u'\xf4','o').replace(u'\xf5','o').replace(u'\xf6','o').replace(u'\xf8','o').replace(u'\u014d','o').replace(u'\u014c','O').replace(u'\xd6','O').replace(u'\u01a1','o')\
            .replace(u'\u201d','').replace(u'\u2013','').replace(u'\u2019','').replace(u'\xa0','')\
            .replace(u'\xf1','n').replace(u'\u0144','n').replace(u'\u0148','n')\
            .replace(u'\u0142','l').replace(u'\u013d','L').replace(u'\u013e','l') \
            .replace(u'\u0137', 'k')\
            .replace(u'\u012b','i').replace(u'\u0131','i').replace(u'\xec','i').replace(u'\xed','i').replace(u'\xee','i').replace(u'\xef','i')\
            .replace(u'\u011f','g').replace(u'\u0121','g')\
            .replace(u'\u0130','f')\
            .replace(u'\xc9','E').replace(u'\xe8','e').replace(u'\xe9','e').replace(u'\xea','e').replace(u'\xeb','e').replace(u'\u1ec7','e').replace(u'\u1ecb','e').replace(u'\u1ebf','e').replace(u'\u0113','e').replace(u'\u0117','e').replace(u'\u011b','e').replace(u'\u0119','e')\
            .replace(u'\u0110','D')\
            .replace(u'\xe7','c').replace(u'\xc7','C').replace(u'\u010d','c').replace(u'\u010c','C').replace(u'\u0107','c')\
            .replace(u'\xdf','B')\
            .replace(u'\xc1','A').replace(u'\xc2','A').replace(u'\xc4','A').replace(u'\xc5','A').replace(u'\xe1','a').replace(u'\xe6','a').replace(u'\u0101','a').replace(u'\u0103','a').replace(u'\u0105','a').replace(u'\u1ea3','a').replace(u'\xe0','a').replace(u'\xe1','a').replace(u'\xe2','a').replace(u'\xe3','a').replace(u'\xe4','a').replace(u'\xe5','a') \
            .replace(u'\xc3', '').replace(u'\xa1', '').replace(u'\xaf', '\'')
    return s