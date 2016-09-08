import numpy as np


class Server():
    """The collection of attributes and representations of a
    server. A server has number of drives (num_drives) and each drive
    supports a number of IOPS (num_iops)"""

    def __init__(self, num_drives, num_iops):
        self.num_drives = num_drives
        self.drive = Drive(num_iops)
        print('This server has {num_drives} drives, each drive supports {num_iops} IOPS'.format(
            num_drives=self.num_drives, num_iops=self.drive.get_num_iops()))

    def num_iops(self, num_secs):
        return self.num_drives * self.drive.iops_time(num_secs)


class Drive():
    """A drive has number of IOPS (num_iops) that it can """

    def __init__(self, num_iops):
        self.num_iops = num_iops

    def get_num_iops(self):
        return self.num_iops

    def iops_time(self, num_secs=1):
        return self.num_iops * num_secs

a_server = Server(64, 150)
a_drive = Drive(150)
#print(a_server.num_drives * a_drive.iops_time(24 * 3600 / 1e+6))
#print(a_drive.iops_time(24 * 3600 / 1e+6))
print('The server can handle', a_server.num_iops(
    24 * 3600 / 1e+6), 'million operations per day')

# 350 million tweets < a_server.num_iops(24*3600/1e+6) < 829.44

# Each tweet is written 40 times, it needs 17 servers
np.ceil(350 * 40 / a_server.num_iops(24 * 3600 / 1e+6))
# How many SANs are needed to store the tweets? Each SAN can support 80k IOPS
350 * 1e+6 / 80000

# Each tweet is written 40 times. How many SANs are needed to store the
# tweets? Each SAN can support 80k IOPS
350 * 1e+6 / 80000 * 40
