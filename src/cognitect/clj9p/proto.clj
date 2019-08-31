; Copyright 2019 Cognitect. All Rights Reserved.
;
; Licensed under the Apache License, Version 2.0 (the "License");
; you may not use this file except in compliance with the License.
; You may obtain a copy of the License at
;
;      http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS-IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.

(ns cognitect.clj9p.proto
  "9P Protocol/Wire constants"
  (:require [clojure.set]))

(def version "9P2000") ;; The 9P Protocol version - V2
(def versionu "9P2000.u") ;; The "Unix Extensions" version, found in Linux's vfs v9fs

(def IOHDRSZ 24)

;; V1 Isn't supported, but the OPs are here for completeness
(def v1 {:tnop     0
         :rnop     1
         :terror   2
         :rerror   3
         :tflush   4
         :rflush   5
         :tclone   6
         :rclone   7
         :twalk    8
         :rwalk    9
         :topen    10
         :ropen    11
         :tcreate  12
         :rcreate  13
         :tread    14
         :rread    15
         :twrite   16
         :rwrite   17
         :tclunk   18
         :rclunk   19
         :tremove  20
         :rremove  21
         :tstat    22
         :rstat    23
         :twstat   24
         :rwstat   25
         :tsession 26
         :rsession 27
         :tattach  28
         :rattach  29})

;; Note that responses (R-messages) are always inc'd transmissions/requests (T-messages)
(def v2 {:tversion 100
         :rversion 101
         :tauth    102
         :rauth    103
         :tattach  104
         :rattach  105
         :terror   106
         :rerror   107
         :tflush   108
         :rflush   109
         :twalk    110
         :rwalk    111
         :topen    112
         :ropen    113
         :tcreate  114
         :rcreate  115
         :tread    116
         :rread    117
         :twrite   118
         :rwrite   119
         :tclunk   120
         :rclunk   121
         :tremove  122
         :rremove  123
         :tstat    124
         :rstat    125
         :twstat   126
         :rwstat   127})
(def v2-codes (clojure.set/map-invert v2))

;; Constants for working with messages and the protocol itself
;; ===========================================================

(def NOTAG 0xffff)
(def NOFID 0xffffffff)

;; -- Open options --
(def OREAD  "Open for read" 0)
(def OWRITE "Open for write" 1)
(def ORDWR  "Open for read and write" 2)
(def OEXEC "Open for Execute, which is read with exec perm check" 3)
(def OTRUNC "Truncate file first; OR'd in" 16)
(def OCEXEC "Close on exec; OR'd in" 32)
(def ORCLOSE "Remove on close; OR'd in" 64)
(def ODIRECT "Direct access; OR'd in" 128)
(def ONONBLOCK "Non-blocking; OR'd in" 256)
(def OEXCL "Exclusive use/Create only; OR'd in" 0x1000)
(def OLOCK "Lock after opening; OR'd in" 0x2000)
(def OAPPEND "Append only; OR'd in" 0x4000)

;; -- Attribute/Access options --
(def AEXIST "Accessible: Exists" 0)
(def AEXEC "Execute access" 1)
(def AWRITE "Write access" 2)
(def AREAD "Read access" 4)

;; -- Permission translation map
(def perm-translation {OREAD AREAD
                       OWRITE AWRITE
                       ORDWR (bit-or AREAD AWRITE)
                       OEXEC AEXEC})

;; -- QID/Type settings --
(def QTDIR "Type bit for directories" 0x80)
(def QTAPPEND "Type bit for append-only files" 0x40)
(def QTEXCL "Type bit for exclusive-use files" 0x20)
(def QTMOUNT "Type bit for mounted channel" 0x10)
(def QTAUTH "Type bit for authentication file" 0x08)
(def QTTMP "Type bit for non-backed-up/transient file" 0x04)
(def QTSYMLINK "Type bit for symbolic link (Unix, 9P2000.u)" 0x02)
(def QTFILE "Type bit for plain file" 0x00)

;; -- File/Entry Modes --
(def DMDIR "Mode bit for directories" 0x80000000)
(def DMAPPEND "Mode bit for append-only files" 0x40000000)
(def DMEXCL "Mode bit for exclusive-use files" 0x20000000)
(def DMMOUNT "Mode bit for mounted channel" 0x10000000)
(def DMAUTH "Mode bit for authentication file" 0x08000000)
(def DMTMP "Mode bit for non-backed-up/transient file" 0x04000000)
(def DMSYMLINK "Mode bit for symbolic link (Unix, 9P2000.u)" 0x02000000)
(def DMDEVICE "Mode bit for device file (Unix, 9P2000.u)" 0x00800000)
(def DMNAMEDPIPE "Mode bit for named pipe (Unix, 9P2000.u)" 0x00200000)
(def DMSOCKET "Mode bit for socket (Unix, 9P2000.u)" 0x00100000)
(def DMSETUID "Mode bit for setuid (Unix, 9P2000.u)" 0x00080000)
(def DMSETGID "Mode bit for setgid (Unix, 9P2000.u)" 0x00040000)
(def DMSTICKY "Mode bit for sticky bit (Unix, 9P2000.u)" 0x00010000)

(def DMEXEC "Mode bit for execute permissions" AEXEC)
(def DMWRITE "Mode bit for write permissions" AWRITE)
(def DMREAD "Mode bit for read permissions" AREAD)

;; -- General definitions/flags --
(def ERRUNDEF "Error: Undefined flag" 0xFFFFFFFF)
(def UIDUNDEF "Undefined flag" 0xFFFFFFFF)

;; -- Authentication --
(def supported-auths #{:pki :sk1})

