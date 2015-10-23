# Copyright (c) 2008-2011 Tim Newsham, Andrey Mirtchovski
# Copyright (c) 2011-2012 Peter V. Saveliev
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
Implementation of basic RSA-key digital signature.

Description:
- Client sends server an Auth message to establish an auth fid.
- Server prepares reads client's public key and generates a random MD5 key
  for the signature encrypting it with the key.
- Client decrypts the hash with its public key, signs it, encrypts the
  signature and sends it to Server
- Server verifies the signature and allows an 'attach' message from client

Public keys are, for now, taken from client's ~/.ssh/id_rsa.pub

This module requires the Python Cryptography Toolkit from
http://www.amk.ca/python/writing/pycrypt/pycrypt.html
"""

import base64
import struct
import os
import random
import getpass
import pickle
import Crypto.Util as util
import hashlib
import sys
from . import utils as c9
from Crypto.Cipher import DES3, AES
from Crypto.PublicKey import RSA, DSA
from Crypto.Util.randpool import RandomPool
from Crypto.Util import number
from Crypto.Hash import MD5
from binascii import unhexlify


class Error(Exception):
    pass


class AuthError(Error):
    pass


class AuthsrvError(Error):
    pass


class BadKeyError(Error):
    pass


class BadKeyPassword(Error):
    pass


class ServerError(Error):
    pass


def gethome(uname):
    for x in open('/etc/passwd').readlines():
        u = x.split(':')
        if uname == u[0]:
            return u[5]


def asn1parse(data):
    things = []
    while data:
        t = ord(data[0])
        assert (t & 0xc0) == 0, 'not a universal value: 0x%02x' % t
        #assert t & 0x20, 'not a constructed value: 0x%02x' % t
        l = ord(data[1])
        assert data != 0x80, "shouldn't be an indefinite length"
        if l & 0x80:  # long form
            ll = l & 0x7f
            l = number.bytes_to_long(data[2:2 + ll])
            s = 2 + ll
        else:
            s = 2
        body, data = data[s:s + l], data[s + l:]
        t = t & (~0x20)
        assert t in (SEQUENCE, INTEGER), 'bad type: 0x%02x' % t
        if t == SEQUENCE:
            things.append(asn1parse(body))
        elif t == INTEGER:
            #assert (ord(body[0])&0x80) == 0, "shouldn't have negative number"
            things.append(number.bytes_to_long(body))
    if len(things) == 1:
        return things[0]
    return things


def asn1pack(data):
    ret = ''
    for part in data:
        if type(part) in (tuple, list):
            partData = asn1pack(part)
            partType = SEQUENCE | 0x20
        elif type(part) in (int, long):
            partData = number.long_to_bytes(part)
            if ord(partData[0]) & (0x80):
                partData = '\x00' + partData
            partType = INTEGER
        else:
            raise 'unknown type %s' % type(part)

        ret += chr(partType)
        if len(partData) > 127:
            l = number.long_to_bytes(len(partData))
            ret += chr(len(l) | 0x80) + l
        else:
            ret += chr(len(partData))
        ret += partData
    return ret

INTEGER = 0x02
SEQUENCE = 0x10

Length = 1024


def NS(t):
    return struct.pack('!L', len(t)) + t


def getNS(s, count=1):
    ns = []
    c = 0
    for i in range(count):
        l, = struct.unpack('!L', s[c:c + 4])
        ns.append(s[c + 4:4 + l + c])
        c += 4 + l
    return tuple(ns) + (s[c:],)


def MP(number):
    if number == 0:
        return '\000' * 4
    assert number > 0
    bn = util.number.long_to_bytes(number)
    if ord(bn[0]) & 128:
        bn = '\000' + bn
    return struct.pack('>L', len(bn)) + bn


def getMP(data):
    """
    get multiple precision integer
    """
    length = struct.unpack('>L', data[:4])[0]
    return util.number.bytes_to_long(data[4:4 + length]), data[4 + length:]


def privkeytostr(key, passphrase=None):
    keyData = '-----BEGIN RSA PRIVATE KEY-----\n'
    p, q = key.p, key.q
    if p > q:
        (p, q) = (q, p)
    # p is less than q
    objData = [0, key.n, key.e, key.d, q, p, key.d % (q - 1),
            key.d % (p - 1), util.number.inverse(p, q)]
    if passphrase:
        iv = RandomPool().get_bytes(8)
        hexiv = ''.join(['%02X' % ord(x) for x in iv])
        keyData += 'Proc-Type: 4,ENCRYPTED\n'
        keyData += 'DEK-Info: DES-EDE3-CBC,%s\n\n' % hexiv
        ba = hashlib.md5(passphrase + iv).digest()
        bb = hashlib.md5(ba + passphrase + iv).digest()
        encKey = (ba + bb)[:24]
    asn1Data = asn1pack([objData])
    if passphrase:
        padLen = 8 - (len(asn1Data) % 8)
        asn1Data += (chr(padLen) * padLen)
        asn1Data = DES3.new(encKey, DES3.MODE_CBC, iv).encrypt(asn1Data)
    b64Data = base64.encodestring(asn1Data).replace('\n', '')
    b64Data = '\n'.join([b64Data[i:i + 64] for i in
        range(0, len(b64Data), 64)])
    keyData += b64Data + '\n'
    keyData += '-----END RSA PRIVATE KEY-----'
    return keyData


def pubkeytostr(key, comment=None):
    keyData = MP(key.e) + MP(key.n)
    b64Data = base64.encodestring(NS("ssh-rsa") + keyData).replace('\n', '')
    return '%s %s %s' % ("ssh-rsa", b64Data, comment)


def strtopubkey(data):
    d = base64.decodestring(data.split(b' ')[1])
    kind, rest = getNS(d)
    if kind == b'ssh-rsa':
        e, rest = getMP(rest)
        n, rest = getMP(rest)
        return RSA.construct((n, e))
    else:
        raise Exception('unknown key type %s' % kind)


def get_key_data(salt, password, keysize):
    keydata = ''
    digest = ''
    # truncate salt
    salt = salt[:8]
    while keysize > 0:
        hash_obj = MD5.new()
        if len(digest) > 0:
            hash_obj.update(digest)
        hash_obj.update(password)
        hash_obj.update(salt)
        digest = hash_obj.digest()
        size = min(keysize, len(digest))
        keydata += digest[:size]
        keysize -= size
    return keydata


def strtoprivkey(data, password):
    kind = data[0][11: 14]
    if data[1].startswith('Proc-Type: 4,ENCRYPTED'):  # encrypted key
        if not password:
            raise BadKeyPassword("password required")
        enc_type, salt = data[2].split(": ")[1].split(",")
        salt = unhexlify(salt.strip())
        b64Data = base64.decodestring(''.join(data[4:-1]))
        if enc_type == "DES-EDE3-CBC":
            key = get_key_data(salt, password, 24)
            keyData = DES3.new(key, DES3.MODE_CBC, salt).decrypt(b64Data)
        elif enc_type == "AES-128-CBC":
            key = get_key_data(salt, password, 16)
            keyData = AES.new(key, AES.MODE_CBC, salt).decrypt(b64Data)
        else:
            raise BadKeyError("unknown encryption")
        removeLen = ord(keyData[-1])
        keyData = keyData[:-removeLen]
    else:
        keyData = base64.decodestring(''.join(data[1:-1]))
    decodedKey = asn1parse(keyData)
    if isinstance(decodedKey[0], list):
        decodedKey = decodedKey[0]  # this happens with encrypted keys
    if kind == 'RSA':
        n, e, d, p, q = decodedKey[1:6]
        return RSA.construct((n, e, d, p, q))
    elif kind == 'DSA':
        p, q, g, y, x = decodedKey[1: 6]
        return DSA.construct((y, g, p, q, x))


def getprivkey(uname, priv=None, passphrase=None):
    if not uname:
        raise AuthError("no uname")

    if priv is None:
        f = gethome(uname)
        if not f:
            raise BadKeyError("no home dir for user %s" % uname)
        f += '/.ssh/id_rsa'
        if not os.path.exists(f):
            raise BadKeyError("no private key and no " + f)
        else:
            privkey = file(f).readlines()
    elif not os.path.exists(priv):
        raise BadKeyError("file not found: " + priv)
    else:
        privkey = file(priv).readlines()

    try:
        return strtoprivkey(privkey, passphrase)
    except BadKeyPassword:
        passphrase = getpass.getpass("password: ")
        return strtoprivkey(privkey, passphrase)


def getchallenge():
    # generate a 16-byte long random string.  (note that the built-
    # in pseudo-random generator uses a 24-bit seed, so this is not
    # as good as it may seem...)
    challenge = map(lambda i: c9.bytes3(chr(random.randint(0x20, 0x7e))), range(16))
    return b''.join(challenge)


class AuthFs(object):
    """
    A special file for performing our pki authentication variant.
    On completion of the protocol, suid is set to the authenticated
    username.
    """
    type = 'pki'
    HaveChal, NeedSign, Success = range(3)
    cancreate = 0
    pubkeys = {}

    def __init__(self, keys=None):
        self.keyfiles = keys or {}
        self.pubkeys = {}

    def addpubkeyfromfile(self, uname, pub):
        pubkey = file(pub).read()
        self.pubkeys[uname] = strtopubkey(pubkey)

    def addpubkey(self, uname, pub):
        self.pubkeys[uname] = strtopubkey(pub)

    def delpubkey(self, uname):
        if uname in self.pubkeys:
            del self.pubkeys[uname]
        else:
            raise BadKeyError("no key for %s" % uname)

    def getpubkey(self, uname, pub=None):
        if not uname:
            raise AuthError('no uname')
        if uname in self.pubkeys:
            return self.pubkeys[uname]
        elif pub is None:
            f = gethome(uname)
            if not f:
                raise BadKeyError("no home for user %s" % uname)
            f += '/.ssh/id_rsa.pub'
            if not os.path.exists(f):
                raise BadKeyError("no public key supplied and no " + f)
            else:
                pubkey = open(f, 'rb').read()
        elif not os.path.exists(pub):
            raise BadKeyError("file not found: " + pub)
        else:
            pubkey = open(pub, 'rb').read()

        self.pubkeys[uname] = strtopubkey(pubkey)
        return self.pubkeys[uname]

    def estab(self, fid):
        fid.suid = None
        fid.phase = self.HaveChal
        if not hasattr(fid, 'uname'):
            raise AuthError("no fid.uname")
        uname = fid.uname.decode('utf-8')
        fid.key = self.getpubkey(uname,
                self.keyfiles.get(uname, None))
        fid.chal = getchallenge()

    def read(self, srv, req):
        f = req.fid
        if f.phase == self.HaveChal:
            f.phase = self.NeedSign
            req.ofcall.data = pickle.dumps(f.key.encrypt(f.chal, ''), protocol=2)
            srv.respond(req, None)
            return
        elif f.phase == self.Success:
            req.ofcall.data = 'success as ' + f.suid
            srv.respond(req, None)
            return
        raise ServerError("unexpected phase")

    def write(self, srv, req):
        f = req.fid
        buf = req.ifcall.data
        if f.phase == self.NeedSign:
            signature = pickle.loads(buf)
            if f.key.verify(f.chal, signature):
                f.phase = self.Success
                f.suid = f.uname
                req.ofcall.count = len(buf)
                srv.respond(req, None)
                return
            else:
                raise ServerError('signature not verified')
        raise ServerError("unexpected phase")


def clientAuth(cl, fcall, credentials):
    pos = [0]

    def rd(l):
        fc = cl._read(fcall.afid, pos[0], l)
        pos[0] += len(fc.data)
        return fc.data

    def wr(x):
        fc = cl._write(fcall.afid, pos[0], x)
        pos[0] += fc.count
        return fc.count

    c = pickle.loads(rd(2048))
    chal = credentials.key.decrypt(c)
    sign = credentials.key.sign(chal, '')

    wr(pickle.dumps(sign, protocol=2))
    return
