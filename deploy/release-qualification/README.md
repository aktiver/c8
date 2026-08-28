# Release qualification jobs

Render `indexed-hpc-job.yaml.tpl` only for non-disruptive cases. It is intentionally parallel and spreads whole-core Rust workers over multiple nodes. Render `serial-disruption-job.yaml.tpl` only into an isolated qualification cluster after recording explicit approval evidence. Both jobs use dense Indexed completion barriers and durable report storage; neither template is installed by the production Helm release.
