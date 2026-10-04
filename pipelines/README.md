# pipelines/

Spark Declarative Pipelines (SDP) projects under study. One subdirectory per
pipeline project, e.g. `pipelines/bronze-silver-gold/`.

Each pipeline project should be runnable on its own and should not assume
any state from `runs/` beyond what its own experiment run explicitly copies
in. Keep pipeline definitions (the SDP spec/config, transforms) separate
from experiment-run outputs, which belong in `runs/<date>/`, not here.

Nothing exists here yet.
