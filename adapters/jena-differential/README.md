# Apache Jena differential adapter

This adapter pins Apache Jena 6.2.0 and implements the Phase 40.13.22 driver envelope for SPARQL 1.1 syntax, TriG syntax, and canonical SELECT/ASK comparison. It uses one JVM per independently bounded case; the partition runner disables nested native pools, imposes wall-time/output ceilings, and supplies absolute, regular-file inputs.

`CONSTRUCT` and `DESCRIBE` graph equivalence is evaluated against the pinned normative W3C result with blank-node canonicalization in the existing native W3C driver. Those forms are never compared using unstable serializer bytes.

Build with `mvn -f adapters/jena-differential/pom.xml package`. Invoke the shaded jar with `java -XX:ActiveProcessorCount=1 -jar .../ngkg-jena-differential.jar request.json`.
