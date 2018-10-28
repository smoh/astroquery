# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
=============
TAP plus
=============

@author: Juan Carlos Segovia
@contact: juan.carlos.segovia@sciops.esa.int

European Space Astronomy Centre (ESAC)
European Space Agency (ESA)

Created on 30 jun. 2016


"""

import unittest
import os
from astroquery.utils.tap.model.tapcolumn import TapColumn


def data_path(filename):
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    return os.path.join(data_dir, filename)


class TestTableColumn(unittest.TestCase):

    def test_column(self):
        flags="flags"
        name = "name"
        arraysize = "arraysize"
        datatype = "datatype"
        flag = "flag"
        ucd = "ucd"
        utype = "utype"
        unit = "unit"
        description = "description"
        c = TapColumn(flags)
        c.name = name
        c.arraysize = arraysize
        c.datatype = datatype
        c.flag = flag
        c.ucd = ucd
        c.unit = unit
        c.utype = utype
        c.description = description
        assert c.name == name, \
            "Invalid name, expected: %s, found: %s" % (name,
                                                       c.name)
        assert c.arraysize == arraysize, \
            "Invalid arraysize, expected: %s, found: %s" % (arraysize,
                                                            c.arraysize)
        assert c.datatype == datatype, \
            "Invalid datatype, expected: %s, found: %s" % (datatype,
                                                           c.datatype)
        assert c.flag == flag, \
            "Invalid flag, expected: %s, found: %s" % (flag,
                                                       c.flag)
        assert c.ucd == ucd, \
            "Invalid ucd, expected: %s, found: %s" % (ucd,
                                                      c.ucd)
        assert c.utype == utype, \
            "Invalid utype, expected: %s, found: %s" % (utype,
                                                        c.utype)
        assert c.unit == unit, \
            "Invalid unit, expected: %s, found: %s" % (unit,
                                                       c.unit)
        assert c.description == description, \
            "Invalid description, expected: %s, found: %s" % (description,
                                                              c.description)
        assert c.flags == flags, \
            "Invalid description, expected: %s, found: %s" % (flags,
                                                              c.flags)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
