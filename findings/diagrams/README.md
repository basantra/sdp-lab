# Diagram sources

Three figures, four formats each, plus the Mermaid used on the Miro board.

| File | Use |
|---|---|
| `.dot` | Graphviz source. Edit this, then re-render. This is the source of truth for the PDF/docx figures. |
| `.svg` | Vector, editable in Illustrator/Figma/Inkscape. Use for web and LinkedIn. |
| `.pdf` | Vector, print. |
| `.png` | Raster at 200 dpi. Embedded in the white paper. |

Re-render after editing a `.dot`:

```
dot -Tpng -Gdpi=200 fig2-refresh-behaviour-two-runs.dot -o fig2-refresh-behaviour-two-runs.png
dot -Tsvg              fig2-refresh-behaviour-two-runs.dot -o fig2-refresh-behaviour-two-runs.svg
dot -Tpdf              fig2-refresh-behaviour-two-runs.dot -o fig2-refresh-behaviour-two-runs.pdf
```

## Two renderers, one set of figures

The Miro board uses Mermaid, rendered by Miro's diagram widget. The white paper uses
Graphviz, because `mermaid-cli` needs a Chrome binary that was not present in the build
container. Same content, same palette, different renderer. If you change a figure, change
it in both places or drop one.

Layout differences that are deliberate, not drift:

- **Figure 1** is left-to-right on Miro, top-to-bottom in the paper. The wide version was
  108px tall at page width, which put the labels below readable size.
- **Figure 2** stacks Run 1 above Run 2 in the paper. Side by side, the part-file names and
  mtimes rendered at roughly 6pt. The evidence has to be legible or the figure is decoration.

## Palette

| Meaning | Fill | Stroke |
|---|---|---|
| Measured on the open build, works | `#adf0c7` | `#087429` |
| Not present upstream, or the failure case | `#ffc6c6` | `#bd0909` |
| Structural: physical table, external trigger | `#c6dcff` | `#305bab` |
| Source data, neutral | `#e7e7e7` | `#595959` |
| Caveat or verdict | `#fff6b6` | `#af7e02` |

Green means measured, red means absent or failing. Keep that mapping if you add a fourth
figure, because the paper's captions rely on it.

## Mermaid sources (Miro board)

Board: https://miro.com/app/board/uXjVHpm378I=/

### Figure 1: SDP scope boundary

```
flowchart LR
    classDef inside fill:#adf0c7,stroke:#087429
    classDef outside fill:#ffc6c6,stroke:#bd0909
    classDef trig fill:#c6dcff,stroke:#305bab

    sched["External scheduler<br/>cron, Airflow, K8s CronJob<br/>no daemon mode exists"]:::trig

    subgraph SDP["Inside Apache SDP 4.2.0"]
      direction TB
      g["Dependency graph resolution"]:::inside
      o["Execution order and parallelism"]:::inside
      st["Streaming tables<br/>incremental ingestion"]:::inside
      mv["Materialized views<br/>FULL recompute every run"]:::inside
      cp["Streaming checkpoints<br/>offset-accurate recovery"]:::inside
      g --> o
      o --> st
      o --> mv
      st --> cp
    end

    subgraph OUT["You still own all of this"]
      direction TB
      b1["Triggering and scheduling"]:::outside
      b2["CDC and SCD Type 1 / Type 2"]:::outside
      b3["Data quality constraints"]:::outside
      b4["Queryable event log, lineage"]:::outside
      b5["Catalog persistence config"]:::outside
      b6["Update flows, foreachBatch sinks"]:::outside
    end

    sched -->|spark-pipelines run| g
```

### Figure 2: refresh behaviour across two runs

```
flowchart TB
    classDef good fill:#adf0c7,stroke:#087429
    classDef bad fill:#ffc6c6,stroke:#bd0909
    classDef src fill:#e7e7e7,stroke:#595959
    classDef verdict fill:#fff6b6,stroke:#af7e02

    subgraph RUN1["Run 1"]
      direction TB
      s1["file1.json, file2.json<br/>land in bucket"]:::src
      st1["Streaming table<br/>batch 0 commits file1, file2"]:::good
      mv1["MV writes part-00000-1316...<br/>mtime 20:51:42, 19.728s"]:::bad
    end

    subgraph RUN2["Run 2, unchanged code, unchanged MV source"]
      direction TB
      s2["file3.json, file4.json<br/>land in bucket"]:::src
      st2["Streaming table<br/>batch 1 commits file3, file4 ONLY<br/>file1 and file2 absent from sources/0/1"]:::good
      mv2["MV DELETES previous part file<br/>writes part-00000-ce36...<br/>mtime 20:52:08, 19.026s"]:::bad
    end

    v1["Streaming tables: incremental<br/>8 rows, zero duplication"]:::good
    v2["Materialized views: full recompute<br/>TriggeredGraphExecution issues FULL, unconditional"]:::bad
    v3["Docs call the default an incremental update.<br/>That names the update MODE, not MV refresh."]:::verdict

    s1 --> st1
    s1 --> mv1
    s2 --> st2
    s2 --> mv2
    st1 --> st2
    mv1 --> mv2
    st2 --> v1
    mv2 --> v2
    v2 --> v3
```

Node `v3` is provisional. It states the docs-versus-measurement tension, and it stands or
falls on the four-run refresh-mode test (default vs `--full-refresh` on the same
materialized view). If that test shows the two modes differ for an MV, delete `v3` and
rewrite the figure.

### Figure 3: stream-to-batch bridge

```
flowchart LR
    classDef ok fill:#adf0c7,stroke:#087429
    classDef blocked fill:#ffc6c6,stroke:#bd0909
    classDef store fill:#c6dcff,stroke:#305bab
    classDef src fill:#e7e7e7,stroke:#595959

    src[(Kafka, cloud storage, rate source)]:::src
    st["Streaming table<br/>@dp.table"]:::ok
    tbl[("Physical table<br/>the ONLY supported crossing")]:::store
    mv["Materialized view<br/>@dp.materialized_view"]:::ok
    tv["Temporary view<br/>@dp.temporary_view"]:::blocked
    err["INVALID_FLOW_QUERY_TYPE.<br/>STREAMING_RELATION_FOR_MATERIALIZED_VIEW<br/>SQLSTATE 42000"]:::blocked

    src --> st
    st --> tbl
    tbl -->|spark.table| mv
    src -.->|rejected| err
    err -.-> mv
    st -.->|does not bridge| tv
    tv -.->|still rejected| mv
```

## Provenance

Every fact on these figures traces to the lab: `runs/2026-09-07/commands.log`,
`findings/01-first-run.md` through `findings/05-verification.md`, and
`env/versions.lock` in `~/code/personal/oci/spark/sdp-lab/`. The Databricks capability
claims are from published documentation and are labelled as such in the paper's
comparison table; nothing on the Databricks side was measured.
