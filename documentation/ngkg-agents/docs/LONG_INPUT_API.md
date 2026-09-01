# Long-input API

The API accepts a prompt, TriG file, source tree archive, PDF, office document,
image, or any other file as immutable bytes. Phase 4 extracts UTF-8 text and
Markdown structure. Other formats remain checksum-bound opaque parts; adding a
parser must not change the stored source object or its source root.

1. Call `POST /v1/agent-inputs` with `sourceName` and `mediaType`.
2. Split the source into bounded parts and `PUT` each zero-based ordinal with
   `x-ngkg-content-sha256`. Retrying the same ordinal/hash is idempotent; a
   different hash is a conflict.
3. Calculate `sourceRootSha256` by hashing the domain bytes
   `ngkg-prompt-source-root-v1\0` followed, in ordinal order, by the signed
   big-endian 32-bit ordinal, signed big-endian 64-bit byte length, and the 64
   ASCII lowercase checksum characters for each part.
4. Call `POST /v1/agent-inputs/{inputId}/finalize` with the part count, total
   bytes, and source root. Finalization is one-way and queues one compilation
   shard per part.
5. Poll `GET /v1/agent-inputs/{inputId}`. `COMPILED` means aggregate source,
   compiled, and requirement roots were atomically frozen.
6. Read the permanent constraint ledger from
   `GET /v1/agent-inputs/{inputId}/requirements`.

The manifest route never returns the internal bucket key. Each request needs a
verified tenant bearer and `agent-inputs:read` or `agent-inputs:write`. Large
payloads are not embedded in MCP JSON; managed agents will refer to `inputId`.

The compiler processes independent parts on multiple Kubernetes nodes. Within
each pod, Rayon uses multiple cores up to the cgroup quota. All merges are
sorted by part ordinal, byte offset and stable ID, so scheduling cannot change
the resulting hashes. Memory mapping is deliberately not used for remote
mutable cache files; cloud objects are streamed and verified first.
