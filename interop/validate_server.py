#!/usr/bin/env python

import sys
import os
import socket
from py9p import py9p

class CmdClient(py9p.Client):
    pass

def getSock(host, port):
    sock = socket.socket(socket.AF_INET)
    sock.connect((host, port),)

def main():
    args = sys.argv[1:]
    srv_host = args[0]
    srv_port = args[1]
    sock = getSock(srv_host, srv_port)
    user = os.environ.get('USER')
    authsrv = None
    authmode = 'none'
    privkey = None
    creds = py9p.Credentials(user, authmode, passwd, privkey)
    print "Version, Auth, and Attach..."
    client = CmdClient(sock, creds, authsrv)
    print "done."
    return 0

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("interrupted.")
    except EOFError:
        print("done.")
    except Exception as m:
        print("unhandled exception: " + str(m.args))
        raise

