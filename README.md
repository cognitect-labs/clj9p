# clj9p

This library is an implementation of the [9P Protocol](https://en.wikipedia.org/wiki/9P).

Bell Labs built an entire vision around "microservices" from 1985-2004 and at the foundation is 9P.

9P, originally built to underlay the Plan9 Distributed Operating System,
is the logical conclusion of the Unix philosophy - "everything is a file."

With 9P everything *really is* a file (synthetic or real) - this includes
devices (CPUs, GPUs), kernel stacks (like the networking stack), distributed services, remote files, ... everything.
The client/consumer decides how to consume all "files" with a userland/client-based
`mount`.  That is, mount unions together all "file systems", producing a single,
resolving, file system upon which the client operates.

For more details read the [9P intro man page](https://swtch.com/plan9port/man/man1/intro.html) or look at the
[example file system](./src/cognitect/clj9p/example.clj)

**NOTE:** This repository also includes useful Netty utilities for building
networked servers and clients.

## Usage

FIXME


## TODO

 * Enhance client `mount` to allow "accruing" routes under a single point, over lifetime
 * Add PKI auth, in the same style as py9p
 * Add DTLS/SSL support via server/client options, passing in an SslContext (created with SslContextBuilder) or some aux fn.
  * Try StartSSL as the default option


## Copyright and License

Copyright © 2019 Cognitect

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

#### Dependencies

 * Clojure and related libraries are released under EPL 1.0
 * Netty is released under Apache 2
 * Parts of the 9P implementation were based on py9p and go9p, both released under BSD

