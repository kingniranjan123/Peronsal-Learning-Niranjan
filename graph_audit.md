# Graph Rendering Audit

## Summary

- Total graph blocks scanned: 106
- Mermaid graph blocks: 106
- Invalid Mermaid blocks after fixes: 0
- ASCII graph blocks in Markdown: 0

## Fixes Applied

- Enabled portal-side Mermaid rendering so blocks such as `graph TD`, `flowchart`, and `sequenceDiagram` render as diagrams instead of code.
- Fixed `Curriculum_DeepDives/pre-formats.md` by quoting Mermaid subgraph labels that contained slashes and parentheses.
- Fixed `fules/detailed_concepts_study_guide_v2.0.md` by simplifying Mermaid edge labels that used unsupported `fit()` / `transform()` syntax.
- Added the supplied RDD runtime architecture image as a curated concept diagram for `rdds` and `rdd-lineage-and-dag`.
- Added a detailed hands-on lab architecture diagram and executable setup guide.

## Validation Artifacts

- Machine-readable scan: `graph_audit.json`
- Mermaid validation result: `graph_validation.json`
- Local screenshots:
  - `localhost_deploy_v1/smoke-hdfs-graph.png`
  - `localhost_deploy_v1/smoke-rdds-curated.png`
  - `localhost_deploy_v1/smoke-hands-on-lab.png`
