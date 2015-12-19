# Copyright (c) 2009-2015, Mendix bv
# All Rights Reserved.
# http://www.mendix.com/
#


class M2EEException(Exception):

    ERR_UNKNOWN = 0x0001

    # 0x01xx configuration errors
    ERR_INVALID_OSGI_CONFIG = 0x0100
    ERR_MISSING_CONFIG = 0x0101

    # 0x02xx start/stop errors
    ERR_START_ALREADY_RUNNING = 0x0200

    def __init__(self, message, cause=None, errno=1):
        self.message = message
        self.cause = cause
        self.errno = errno

    def __str__(self):
        strlist = [self.message]
        if self.cause is not None:
            strlist.append("caused by: %s" % self.cause)
        strlist.append("errno: %s" % hex(self.errno))
        return ', '.join(strlist)
