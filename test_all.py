#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest


if __name__ == "__main__":
    testmodules = [
        "manning.models.mysql.tests.test_MySQLBase",
        "manning.models.sqlite.tests.test_SQLiteBase"
    ]

    suite = unittest.TestSuite()

    for t in testmodules:
        print "testing... %s" % t
        try:
            # If the module defines a suite() function, call it to get the suite.
            mod = __import__(t, globals(), locals(), ['suite'])
            suitefn = getattr(mod, 'suite')
            suite.addTest(suitefn())
        except (ImportError, AttributeError):
            # else, just load all the test cases from the module.
            suite.addTest(unittest.defaultTestLoader.loadTestsFromName(t))

    unittest.TextTestRunner().run(suite)
