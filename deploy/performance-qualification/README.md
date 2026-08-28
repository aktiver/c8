# Performance qualification deployment

Render `indexed-job.yaml.tpl` only after the plan is built. Set `NGKG_BENCHMARK_PARTITIONS` to the plan's exact dense partition count and set the qualification image repository/digest to a signed release artifact. The checked job is deliberately sequential (`parallelism: 1`) so unrelated scenarios do not contaminate each other's measurements; intentional concurrency and multinode load are created inside each content-bound scenario.

The mounted `external-jena-client` is only a bounded client for a separately administered same-hardware baseline. It must not contain Jena libraries and it is not mounted into any NGKG production workload. Remove that argument and Secret only when no plan scenario requires the external baseline; the runner otherwise fails closed.

Kueue admits the job to a benchmark queue. Fixed CPU/memory requests equal limits, the node selector fixes the hardware class, and `maxFailedIndexes: 0` prevents a failed trial from being silently replaced. Raw reports use durable storage and must be retained with the final certificate.
