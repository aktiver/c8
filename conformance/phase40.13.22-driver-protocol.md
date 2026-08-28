# Phase 40.13.22 driver protocol

The partition coordinator invokes each driver as an argument vector (never through a shell) and appends an absolute request JSON path. A driver emits exactly one bounded UTF-8 JSON object on stdout:

```json
{"formatVersion":1,"engine":"apache-jena","engineVersion":"6.2.0","caseId":"https://example/case","outcome":"success","resultSha256":"<64 lowercase hex>","errorClass":null,"complete":true}
```

`engine` is exactly `ngkg`, `w3c-expected`, `apache-jena`, or `hermit`; `engineVersion` must exactly match the content-bound suite inventory. A successful observation contains one canonical result digest and no error. A failed observation contains no result digest and one stable error class. `complete:false`, unknown fields, oversized output, timeout, non-zero process exit, version/identity drift, and mixed result/error evidence fail the partition.

Canonical SELECT output is a multiset of RDF-term bindings, retaining duplicates. Row order is included only where SPARQL makes it significant. ASK hashes the boolean value. CONSTRUCT, DESCRIBE, and TriG datasets use blank-node-independent dataset canonicalization; serializer bytes are not evidence. Negative tests compare terminal error classes and prohibit partial answers.
