# nf-meta-sample-rename

A reusable Nextflow connector that stages the file(s) referenced by each row of
a samplesheet under filenames rendered from a template (e.g. to satisfy a
downstream tool's strict naming convention, such as Cell Ranger's mkfastq-style
`<Sample>_S1_L001_R1_001.fastq.gz`), and writes a new samplesheet pointing at
the renamed files. Every other column is passed through unchanged.

Implemented as a single named workflow, `SAMPLE_RENAME`, with explicit
`take:`/`emit:` inputs/outputs so it can be `include`d directly into another
pipeline, plus a thin top-level `workflow {}` that forwards CLI params into it
for standalone use.

Not specific to fastq files or paired-end reads -- `read_cols` accepts any
number of columns (single-end, paired-end, index reads, ...), and
`name_template` placeholders are just samplesheet column names, so any
metadata column (donor, batch, lane, ...) can be woven into the output name.

## Usage

```bash
nextflow run https://github.com/bmds-tue/nf-meta-sample-rename \
  --samplesheet  fetchngs_samplesheet.csv \
  --read_cols    fastq_1=R1,fastq_2=R2 \
  --name_template 10x_mkfastq \
  --outdir       results
```

## Output layout

```
${outdir}/
  samples/
    <rendered filenames>
  samplesheet/
    <outname>          # default: samplesheet.csv
```

## Params

| Param | Default | Description |
|---|---|---|
| `samplesheet` | (required) | Path to the input CSV/TSV. Delimiter auto-detected from extension. |
| `read_cols` | (required) | Comma-separated `column=label` pairs, e.g. `fastq_1=R1,fastq_2=R2`. Identifies which columns hold files to rename and the label each contributes to the `{read}` placeholder. Not limited to two columns/paired-end -- add e.g. `index=I1` for an index read. A row may leave any of these columns blank (mixed single-/paired-end in one sheet); it's only an error if *all* configured columns are blank for a row. |
| `name_template` | (required) | Output filename template, or a built-in preset name. See below. |
| `file_op` | `symlink` | `symlink`\|`link`\|`copy` -- how a renamed file relates to its original. See below. |
| `outdir` | (required) | Renamed files go to `<outdir>/samples/`, the output samplesheet to `<outdir>/samplesheet/`. |
| `outname` | `samplesheet.csv` | Output samplesheet filename; extension controls the output delimiter. |
| `quote` | `"` | Quote character for parsing input and quoting output fields. |

## `name_template`

A value containing `{` is used as a literal template, e.g.:

```
--name_template '{sample}_{read}.fastq.gz'
```

Placeholders are **any column name from the input samplesheet** (e.g.
`{sample}`, `{donor}`, `{lane}` -- whatever your sheet has), plus one synthetic
placeholder, `{read}`, which isn't a column value: it's the label configured
for whichever `read_cols` column a given file came from. Constant parts of a
convention that never vary (like Cell Ranger's `S1`/`L001` tokens) don't need
placeholders at all -- just write them literally in the template. If a
convention needs them to vary per row (e.g. real per-lane numbers), add a
column for it to your samplesheet and reference it (`{lane}`).

Every placeholder in the template is validated against the samplesheet's
actual columns (plus `read`) before anything is staged -- an unknown
placeholder fails fast with the exact bad token named, rather than silently
leaving `{typo}` in the output filename.

A value with **no** `{` is looked up as a preset name instead:

| Preset | Expands to |
|---|---|
| `10x_mkfastq` | `{sample}_S1_L001_{read}_001.fastq.gz` |

(`10x_mkfastq` requires the samplesheet to have a column literally named
`sample` -- e.g. nf-core/fetchngs' own output samplesheet already does.)

**Filename collisions** across all rows/columns are checked before any file is
staged -- if the template doesn't produce a unique name per file (e.g. two
lanes of the same sample with no distinguishing column), the run fails with the
colliding names listed rather than silently overwriting one with the other.

## `file_op`

How each renamed file relates to its original:

| Value | Mechanism | When to use |
|---|---|---|
| `symlink` (default) | `ln -s` | Fast, no data duplication. Matches the manual rename workaround this adapter replaces. |
| `link` | `ln` (hardlink) | Same filesystem only; behaves as a genuine regular file for tools that refuse to follow symlinks. |
| `copy` | `cp` | Always portable; needed when crossing storage backends, at the cost of duplicating the file's bytes. |

Renamed files are written directly under `<outdir>/samples/` (not via
`publishDir`) so `file_op`'s meaning is unambiguous -- chaining our own
symlink/link/copy with a second publishDir-level transformation risks
double-hop symlink chains or silently dereferencing a symlink into a full copy.
The original's real path is resolved (`toRealPath()`) before linking/copying,
so a `symlink` request points straight at the true source file rather than
through Nextflow's own staging symlink.

**Known limitation:** because of the above, `outdir` must be reachable from
wherever the task actually executes (fine for local/Docker/shared-filesystem
HPC, which is how this connector is used today; not for isolated cloud
executors with no shared mount to an arbitrary output path). This is the same
assumption `nf-meta-samplesheet-ops`' `collectFile(storeDir:)` already makes,
not a new constraint.

## Test

```bash
nf-test test test/main.nf.test
```
