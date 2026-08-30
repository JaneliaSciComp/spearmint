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
