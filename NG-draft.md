# Next Gen

@README.md:37-38 hash tag in redis cluster takes first `{}` as hash tag,
so if we want events related to a namespace go to the same server this is ok

I have the following convention

* fixed number of delimeters
* namespaces are alphanumeric (plus `-`) tenant-like identifier
* basename is the only free form part of the key, everything else is fixed.
* use `/` as delimiter because `:` might be in basename but `/` can't be in basename
* basename can't have new line or `"*?[]{}\` (for example `*` would affect pubsub commands..etc)
* basename can't be escaped with `\` because `\*` still contains `*`
* space breaks some cli commands so we might consider it special too. and all ascii control characters are special too `0x00-0x20`
* if url encoding is used, we must to escape `%` and `+` too (because some decoders might convert + to space)
* either urlencode-based `%xx` (in that case % would also become special) or json-based (`\xXX` or `\uXXXX`) to be decided
* use `parentId=0` and `basename=''` for filesystem a wide keys
* `kvfs` is magic prefix that is fixed (in the future if we broken the protocol it might become `kvfs.v2`)
* `kvfs/<INFX>/<SFX>` where
  * SFX is a suffix one of `counter|pubsub|meta|dir|data` (we might have more in the future)
  * INFX is an infix in the form of `<namespace>/<parentId>/<basename>`
  * based on sharding level some parts of INFX got wrapped within `{}`
  * example: `kvfs/{default}/1/README.md/data` shared by namespace
  * example: `kvfs/{default/1}/README.md/data` shared by namespace and directory
  * example: `kvfs/{default/1/README.me}/data` shared by namespace and directory and file (becareful if basename contains `{}`)


the default sharding level is at namespace level.

```
kvfs/{<ns>}/0//counter                    INCR-only id counter
kvfs/{<ns>}/0//pubsub                     global PUB/SUB channel; payload "{parentId}:{basename}"
kvfs/{<ns>}/<parentId>/<basename>/meta    JSON metadata (includes immutable id)
kvfs/{<ns>}/<parentId>/<basename>/dir     SET of child basenames (dirs)
kvfs/{<ns>}/<parentId>/<basename>/data    file bytes or symlink target
```

I'm not sure if file-level sharding is working, we might need to double check that directory rename is still possible accrss shards.

## Filesystem book-keeping keys

sfx=counter and sfx=pubsub with special `parentId=0` and empty `basename=''` used for special book-keeping like:

* detect initialization
* what is the next available inode (INCR sfx=counter)
* publish meta data changes events to cluster-wide subscribers

## json decode (json.loads) or url decode (urllib.parse.unquote)

objectives: since redis is binary we don't need to care about escaping, we should not make it convert a single character to multiple characters just for the sake of escaping.

I want most files stored as-is without any escaping, but some files might need to be escaped.

given
* no custom decoder is needed, we can use the built-in `json.loads` or `urllib.parse.unquote`
* custom encoder is needed in both cases.
* we careful of `.`, `..` they are allowed as in `.env` and `foo.jpg` we don't want to escape them
* we will never see `/` in basename
* since we don't need to deal with `:` escaping we can json encode the basename instead of urlencode (json encode is more efficient than urlencode for unicode strings)
* json decoding is optional and only done is starts with `"`
* if the key has new line or any of the following `"*?[]{}\` then json is activated and those must be escaped using `\xXX` where `XX` is the hex code of the character or `\uXXXX` where `XXXX` is the unicode code point of the character.

### Decision

* we are going with url-encoding not json encoding
* we are going to use custom encoder that only
  * `"*?[]{}\` because of glob patterns in redis sub/sub
  * `\n` new-line just in-case along with all invisible ascii `0x00-0x1f` (up to but not including space)
  * verbatim `%` because we are doing url encoding.
  * NOTE: `/` should be escaped for completeness, but it's not allowed in basenames.
* we are going to use builtin standard decoder

## Implementation

* pick convention and write it down in `AGENTS.md` either `parentId` or `parent_id` based on current code.
* have a unified helper named `get_key()` that takes
  * `namespace` and `shardingLevel` can be global variables, set once, and never get modified, if language and best practices allow that.
  * `parentId`
  * `basename`
  * `sfx` which can be empty string or `counter|pubsub|meta|data|dir`
* `sfx` can be set to empty string and sfx is added by the caller, so that the prefix is calculated once.
 

### Service start

* fuse service must recieve namespace and shardingLevel and those shall not change in the whole service process
* use NX flag (set if not exists) `get_key(parent_id=0, basename='', sfx='counter')` to set a value of 0 
  * if set failed because key already set, then it's already initialized, skip initialization
  * if set then start initialization
* before initializing a new filesystem, make sure no other name space exists with different sharding level
  * `kvfs/{default}/0//counter` - sharding_level=namespace
  * `kvfs/{default/0}//counter` - sharding_level=directory
  * `kvfs/{default/0/}/counter` - sharding_level=file
  * if exists exit service with proper error message you can't just change level on existing filesystem.
* In Linux FUSE 3, the root directory inode is conventionally 1, not 0. FUSE_ROOT_ID is defined as 1.
  * should the set NX be to 1 not zero, and all the `//counter` `//pubsub` be on 1 instead of 0.
  * should base name of the root of the filesystem be empty string or `.` that is `/./counter` and `/./pubsub`?
  * should the list of files in the root of the filesystem be smembers of `/./dir` `//dir` and should they be under 0 or 1 parent id
  * should filesystem counter and pubsub live as suffixes to the root node or in their own 0 book-keeping keys
* I think the basename of the root of the filesystem should be '.' so that when concatinated to form full path they won't become absolute `./path/to/something` vs. `/path/to/something`

### directories

* `sfx='dir'` is a redis set with `smembers` the basenames of children
* `mkdir FS_ROOT/path/to/parent/child`
  * find the `parentId` of the parent
  * do `sadd` of `child` to `parentId` `/dir`
  * set sfx=meta and sfx=dir with empty 


