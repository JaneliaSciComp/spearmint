# Run directories: rerun-in-place vs always-fresh + seed

Status: OPEN -- discussion doc, nothing decided. Companion to sidecar.md (same spirit:
weigh the models before touching code).

## The question

Today a rerun can land in a PREVIOUS run's directory: `extend` resumes into job_key's most
recent outdir, `replace` rmtree's that same dir and starts empty in-place. The alternative:
every run gets a fresh `run00NNN` dir, always, and "resuming" means copying or linking the
resources you need (checkpoints, caches, preprocessed data) from a previous dir into the new
one. Which model should spearmint commit to?

## Current semantics (for reference)

- `new`: mint fresh `run00NNN`, empty.
- `extend`: new ledger row, but `outdir` = the job_key's most recent outdir. Script sees its
  own prior files and resumes. Multiple rows share one dir.
- `replace`: same dir reuse, but the old contents are rmtree'd first -- start-from-empty
  in-place. Older `new` dirs are untouched; only the attempt being superseded is cleared.
- Rows store ROOT-relative outdirs; a wip-guard refuses extend/replace while a live run holds
  the dir.

## Scar tissue already collected (evidence, not hypotheticals)

- **The replace race.** `replace` cleared a dir while the live report's `savedir()` still
  pointed at it -- every mid-run render died on FileNotFoundError and the report silently
  froze. The fix was a DISCIPLINE, not a mechanism: exists()-guard every read in every report
  fn, forever. Disciplines that every user must remember are design debt.
- **Diff needs distinct dirs.** The `/diff` page and the run page's "diff vs previous" link
  had to dedupe outdirs because extend reuses them -- "compare this run to the last one" is
  literally impossible after extend/replace: the earlier state is gone or entangled.
- **Provenance ambiguity.** Two+ ledger rows sharing one dir means "which run produced this
  file?" has no answer. mtime forensics at best.

## Option A: keep rerun-in-place (extend/replace as today)

Pros:
- **Zero disk overhead.** A 50GB checkpoint dir resumed 10 times costs 50GB, not 500GB.
  On group quota'd GPFS this is not a nicety.
- **Zero-friction resume.** Training scripts see exactly the files they wrote, at the paths
  they wrote them. No seeding step, no spearmint API for the script to learn -- a bare
  hydra/argparse script resumes with no changes. This is the invariant we've defended
  hardest: workers stay plain.
- **Stable paths.** Bookmarks, tensorboard invocations, `tail -f`, external tools keep
  working across a resume.
- **No copy/link semantics to get wrong** (see Option B's cons -- they're real).

Cons:
- **History is destroyed or entangled.** replace deletes it; extend mutates it. The ledger's
  whole value proposition is "immutable record of what ran"; shared dirs undermine it at the
  filesystem layer.
- **Live readers race with rmtree** (the report freeze). Any future reader -- dashboard,
  diff, a sidecar validator -- inherits the exists()-guard discipline.
- **Crash mid-anything leaves a hybrid dir.** A replace that dies after rmtree but mid-write,
  or an extend that dies mid-checkpoint, leaves a dir that is neither the old run nor a new
  one, and the ledger can't tell you which files belong to which attempt.
- **Diff/compare permanently crippled** for exactly the runs where you most want it
  ("did resuming with the new LR help?").

## Option B: always a fresh dir; seed via copy/link

Every run is `new`. "extend" becomes: fresh dir + the needed prior artifacts made visible in
it. "replace" becomes just... `new` (the old dir stays; gc handles disk later).

Pros:
- **Immutable history.** run row <-> dir is 1:1, forever. Diff always works, provenance is
  trivial, the ledger's story and the filesystem's story agree.
- **No reader races, no exists()-guard discipline.** Dirs never vanish or mutate under a
  reader. The report-fn contract gets simpler.
- **Crash-safe.** A dead run leaves a partial NEW dir; the previous good run is untouched.
  Retry logic and humans both love this.
- **replace-regret is recoverable** -- the superseded dir still exists until gc.

Cons:
- **Disk, disk, disk.** Copying checkpoints per resume multiplies usage. Mitigations each
  have teeth:
  - symlink: if the resumed script OVERWRITES the linked file (checkpoint loops do exactly
    this), it mutates the previous run THROUGH the link -- worse than Option A because it
    corrupts history while claiming to preserve it. Only safe for read-only inputs.
  - hardlink: same overwrite hazard unless the writer replaces-by-rename (most torch.save
    paths do write-to-temp+rename, which breaks the link safely -- but "most" is a
    discipline again).
  - reflink/CoW: perfect where it exists; APFS yes, GPFS/NFS at Janelia effectively no.
- **Seeding is a new mechanism** with real API surface: what gets seeded (whole dir? a
  manifest? glob?), when (before launch), by whom (driver -- workers must stay plain), and it
  must be recorded in the ledger (a seeded file is an input -> staleness).
- **Path churn.** "The" checkpoint now moves every resume; anything not going through
  `latest_outdir` breaks. (We already route everything through the ledger, so this is small.)
- **gc becomes load-bearing.** Today disk is bounded by reuse; under B it grows per attempt
  until gc runs. A gc that deletes wrongly is scarier than any race in Option A.

## Middle grounds worth naming

1. **B + `prev` symlink, read-only discipline.** Fresh dir always; driver drops a single
   `prev -> ../run00NNN` symlink to the seed run. Scripts READ `prev/ckpt.pt`, WRITE
   `./ckpt.pt`. One convention, no copying, no manifest API, history immutable. The
   discipline ("never write through prev/") is enforceable-ish: driver could chmod the
   target read-only... on second thought no -- that mutates the old run's dir. It stays a
   convention, but a much smaller one than exists()-guard-everything.
2. **B with copy-for-writable, link-for-readonly** via a tiny per-stage seed spec. Most
   correct, most machinery. Feels like the config-language trap we keep refusing.
3. **A, but replace renames instead of clearing:** `run00042` -> `run00042.old` (or into a
   `.trash/`), then a fresh empty `run00042`. Kills the rmtree race and the regret case
   while keeping stable paths. Doesn't fix extend's entanglement or provenance.
4. **Split the verbs:** `replace` = B (it never really needed the old dir, it wanted the old
   dir GONE -- fresh dir + gc-eligible old one is strictly better). `extend` = A (it's the
   one with a genuine claim on in-place: zero-copy resume of plain workers). Smallest change
   that removes rmtree from the hot path entirely.

## Current lean (not a decision)

Middle ground 4 first -- make `replace` mint a fresh dir and merely mark the superseded one
(gc later); rmtree leaves the launch path, the report race class dies, diff-after-replace
works. Then evaluate 1 (`prev` symlink) as the future of `extend`: it buys immutable history
for one small convention, and plain workers stay plain (a symlink is just a path). Full B
with seed manifests (2) only if real resume workloads prove the `prev` convention too weak.

Open questions before deciding:
- How big are real resume artifacts in mia-muvit / lsd_neuron (copy cost if we ever copy)?
- Does anything today depend on extend's stable paths (external scripts, tensorboard)?
- What does gc look like under B -- age-based? keep-last-N per job_key? Who runs it?
- Ledger schema: does a run need a `seeded_from` column for provenance under B?

## Related: folders as the record vs a central metrics database

The dir-vs-dir question above assumes the run DIRECTORY is where metrics, curves, and PNGs
live at all. The alternative worth naming: a central database records everything -- every
metric point, every image -- and dirs hold only bulk artifacts (checkpoints, zarrs). This is
the wandb/mlflow model. Spearmint is already a hybrid (rundb.db for run METADATA, files for
everything else), so the real question is where the line sits.

### What folders buy

- **Schema changes are free.** metrics.jsonl has no schema: a new metric is a new key on the
  next line; a renamed one just starts appearing. Old runs keep their old shape and stay
  readable forever -- viz.lines already does column-union + per-series drop-missing for
  exactly this reason. A central db needs migrations, or an EAV/key-value table
  (`run, step, name, value`)... which is jsonl with more ceremony. ML metric schemas churn
  every week of a project; this is the folder model's strongest card.
- **No writer contention.** 200 array jobs each append to their own file. A central sqlite on
  GPFS/NFS is single-writer with network-fs locking -- we already dodged this once: only the
  DRIVER writes rundb.db, workers get env identity, precisely because concurrent sqlite over
  network filesystems is a known hazard. Central metrics would put every training step's
  write on that path (or force a collector service, see below).
- **Unix tooling and zero-dependency debugging.** `tail -f metrics.jsonl`, `ls`, `rsync`,
  `open *.png` all work with spearmint absent or broken. A db needs the tool alive and a
  query language to answer "what did this run even produce?"
- **Locality and lifecycle alignment.** The curve sits next to the checkpoint and stderr that
  produced it; a run dir is self-describing; `rm -rf` (or gc) deletes a run and its record
  agrees with itself. Central db + dirs means two lifecycles to keep in sync -- delete the
  dir and the db still claims the PNGs exist, or vice versa.
- **Failure isolation.** A torn jsonl line loses one line of one run (readers skip it). A
  corrupted central db loses the record of every experiment you've ever run.
- **Composes with trees that already exist.** explorer's plain mode browses ANY nested output
  dir -- wandb dumps, tensorboard logs, a collaborator's ad-hoc tree -- with zero ingestion.
  A db renders only what was ingested through its API; everything else is invisible.
- **PNGs in a db are blobs**: you pay db bloat AND still extract to view. Nobody wins.

### What a central db buys

- **Cross-run queries.** "min val_loss over all runs where lr < 1e-3, grouped by
  augmentation" is one SQL statement. Folder-based means walking N dirs and parsing N files.
- **Hyperparam searches are the pressure point.** At 500 trials, "sort by final val_loss"
  against 500 summary.json files means 500 opens per dashboard render. This is the one
  workload where the folder model degrades measurably -- though note it's the small
  SUMMARIES you need indexed, never the per-step curves or PNGs (you look at curves a few
  runs at a time, always).
- **Uniform schema enables generic UIs** -- parallel coordinates, run tables sortable by any
  metric. But observe what wandb actually does under the hood: schemaless key-value logging,
  i.e. jsonl-in-a-db. The generic UI comes from the key-value shape, not from centralism.
- **Point atomicity / consistency** vs torn writes. Real but small: single-line jsonl appends
  are effectively atomic, and readers that skip a bad trailing line (ours do) close the gap.
- **A service-backed db** (wandb, mlflow server) additionally buys multi-user and web access
  -- and is flatly out: spearmint's invariants are stdlib-only, no services, workers stay
  plain. A serverless central sqlite avoids the service but inherits the writer-contention
  problem above.

### Where the line should sit (lean)

The current split is close to right and is itself the answer: a small central db for the
RELATIONAL CORE with a stable, spearmint-owned schema (runs, status, argv, commits,
staleness -- rundb.db), files for everything whose schema the USER owns and churns (metrics,
images, tables). The one genuine gap is hyperparam-search-scale summary queries, and the fix
is an INDEX, not a migration: at finalize, the driver (already the sole db writer) copies
summary.json key-values into a ledger table. It's a derived cache -- files remain the truth,
old runs backfill by re-scan, a dropped table costs nothing. That captures the db's only
killer feature without giving up schema freedom, unix debuggability, or the plain-worker
invariant.
