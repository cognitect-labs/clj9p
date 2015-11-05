Attempting to separate the different facets of what makes up a file
server. Noodling on types as a shorthand way to express the different responsibilities.

;; QidMapping m
;; ->qid :: m -> r -> Qid
;; qid-> :: m -> Qid -> r

;; Authority
;; resolve :: Authority -> String -> Principal

;; Permission p
;; Principal = (user, [groups])
;; readable? :: Permission -> Principal -> Bool
;; writable? :: Permission -> Principal -> Bool
;; executable? :: Permission -> Principal -> Bool
;; user :: Permission -> User
;; group :: Permission -> Group
;; data operation = Read | Write | Execute
;; attempt :: (Permissioned r, Operation op) => r -> Principal -> op -> ( r -> x ) -> x | Error

;; the entries here must get fids assigned, and can then have any
;; other operation applied. that means they are congruent with
;; resources. e == r

;; Hierarchy h
;; root :: h -> e
;; child :: h -> e -> ( e -> Bool ) -> e
;; parent :: h -> e -> Maybe e
;; children :: h -> e -> [e]

;; class Permissioned r where
;;    permission :: r -> Permission

;; Filesystem fs
;; Resource r (type tbd by the fs)
;; random-access? :: r -> Bool
;; exclusive? :: r -> Bool
;; directory? :: r -> Bool
;; size :: r -> Long
;; modified :: r -> Instant
;; created :: r -> Instant
;; permission :: r -> Permission
;; read  :: r -> int -> int -> buffer

;; change-permission :: FileSystem r -> r -> (FileSystem r, r)
;; create :: FileSystem r -> String -> (FileSystem r, r)
;; create-directory :: FileSystem r -> String -> (FileSystem r, r)
;; write :: FileSystem r -> r -> int -> int -> buffer -> (FileSystem r, int)
;; truncate :: FileSystem r -> r -> (FileSystem r, r)
;; delete :: FileSystem r -> r -> (FileSystem r, r)

;; ExclusionRealm er
;; Exclusive e
;; lock :: ExclusionRealm -> Exclusive -> (ExclusionRealm, Lock e)
;; unlock :: Lock e -> ExclusionRealm
