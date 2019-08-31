# Copyright 2019 Cognitect. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# This is an example of using 9P to run OpenCL kernels through a GPU
# via a synthetic file system

# First, we need a server to populate with the files.
# We'll have three files:
#  * /requests
#  * /working
#  * /results

# Note that this idea works well for jobbing by replacing the files
# with directories (ala the example given in "Grave Robbers from Outer Space
# Using 9P2000 Under Linux")

import time
import os
import sys
import copy
from py9p import py9p

###
# 9P auxiliary functions
##############################

if sys.version_info[0] == 2:
    def bytes3(x):
        return bytes(x)
else:
    def bytes3(x):
        return bytes(x, 'utf-8')

def rootdir():
    """Build a valid 9P Directory to serve as a root directory"""
    # Assumes the protocol version is .u/dotu 0; ie: This is legacy, not .u
    rdir = py9p.Dir()
    rdir.children = []
    rdir.type = 0
    rdir.dev = 0
    rdir.mode = 0o20000000755
    rdir.atime = rdir.mtime = int(time.time())
    rdir.length = 0
    rdir.name = '/'
    rdir.uid = rdir.gid = rdir.muid = bytes3(os.environ['USER'])
    rdir.qid = py9p.Qid(py9p.QTDIR, 0, py9p.hash8(rdir.name))
    rdir.parent = rdir # / is its own parent, just so we don't fall off the edge of the earth
    return rdir

def NinePFile(name, parent, byte_length):
    f = copy.copy(parent)
    f.name = name
    f.qid = py9p.Qid(0, 0, py9p.hash8(f.name))
    f.length = byte_length
    f.parent = parent
    f.mode = 0o644
    return f

###
# 9P Server
##############

# Arguably, a lot of this pretty hacky (checking the filename str for dispatch),
# but it proves the general concept

class Srv (py9p.Server):

    files = {}

    def __init__(self, mountpoint="/"):
        self.mountpoint = mountpoint
        # Create the root directory
        self.root = rootdir()

        # Using a shallow-copy of the root, add two files:
        # `hello`, `goodbye`
        hello_file = NinePFile("hello", self.root, 1024)
        self.root.children.append(hello_file)
        goodbye_file = NinePFile("goodbye", self.root, 1024)
        self.root.children.append(goodbye_file)

        self.files[self.root.qid.path] = self.root
        for child in self.root.children:
            self.files[child.qid.path] = child

    def open(self, srv, req):
        '''If we have a file tree then simply check whether the Qid matches
        anything inside. respond qid and iounit are set by protocol'''
        if req.fid.qid.path not in self.files:
            srv.respond(req, "unknown file")
        f = self.files[req.fid.qid.path]
        if (req.ifcall.mode & f.mode) != py9p.OREAD :
            raise py9p.ServerError("permission denied")
        srv.respond(req, None)

    def walk(self, srv, req):
        # root walks are handled inside the protocol if we have self.root
        # set, so don't do them here. '..' however is handled by us

        f = self.files[req.fid.qid.path]
        if len(req.ifcall.wname) > 1:
            srv.respond(req, "don't know how to handle multiple walks yet")
            return

        if req.ifcall.wname[0] == '..':
            req.ofcall.wqid.append(f.parent.qid)
            srv.respond(req, None)
            return

        for x in f.children:
            if req.ifcall.wname[0] == x.name:
                req.ofcall.wqid.append(x.qid)
                srv.respond(req, None)
                return

        srv.respond(req, "can't find %s"%req.ifcall.wname[0])
        return

    def stat(self, srv, req):
        if req.fid.qid.path not in self.files:
            raise py9p.ServerError("unknown file")
        req.ofcall.stat.append(self.files[req.fid.qid.path])
        srv.respond(req, None)

    def read(self, srv, req):
        if req.fid.qid.path not in self.files:
            raise py9p.ServerError("unknown file")

        f = self.files[req.fid.qid.path]
        if f.qid.type & py9p.QTDIR:
            req.ofcall.stat = []
            for x in f.children:
                req.ofcall.stat.append(x)
        elif f.name == 'hello':
            if req.ifcall.offset == 0:
                buf = 'Hello World\n'
                req.ofcall.data = buf[:req.ifcall.count]
            else:
                req.ofcall.data = ''
        elif f.name == 'goodbye':
            if req.ifcall.offset == 0:
                buf = 'Goodbye and Goodnight\n'
                req.ofcall.data = buf[:req.ifcall.count]
            else:
                req.ofcall.data = ''

        srv.respond(req, None)

def main(prog, *args):
    listen = 'localhost'
    #port = py9p.PORT
    port = 9090
    mods = []
    noauth = 0
    dbg = False
    user = None
    dom = None
    passwd = None
    authmode = None
    key = None
    dotu = 0

    print "Starting the server: "+str(listen)+":"+str(port)
    srv = py9p.Server(listen=(listen, port), authmode=authmode, user=user, dom=dom, key=key, chatty=dbg)
    print "Mounting the filesystem..."
    srv.mount(Srv())
    print "Serving requests..."
    srv.serve()


if __name__ == "__main__" :
    try :
        main(*sys.argv)
    except KeyboardInterrupt :
        print("interrupted.")

