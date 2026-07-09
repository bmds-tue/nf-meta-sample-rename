nextflow.enable.dsl = 2

include { validateParameters; paramsHelp } from 'plugin/nf-schema'

params.help          = false
params.samplesheet   = null
params.read_cols     = null
params.name_template = null
params.file_op       = 'symlink'
params.outdir        = null
params.outname       = 'samplesheet.csv'
params.quote         = '"'

// Built-in name_template shortcuts. A name_template value that doesn't contain
// '{' is looked up here instead of being treated as a literal template.
def presets() {
    return [
        '10x_mkfastq': '{sample}_S1_L001_{read}_001.fastq.gz',
    ]
}

// Also used for the output file: the delimiter is derived from whichever path/name is passed in.
def detectSep(path) {
    def name = path.toString().toLowerCase()
    if (name.endsWith('.csv')) return ','
    if (name.endsWith('.tsv') || name.endsWith('.tab')) return '\t'
    error "Cannot detect delimiter for '${path}' -- expected a .csv or .tsv/.tab extension"
}

def splitList(s) {
    if (!s) return []
    return s.split(',')*.trim()
}

// Quotes a single output field if it contains the delimiter, the quote character,
// or a newline -- otherwise CSV/TSV rows silently shift columns whenever a value
// happens to contain a comma/tab.
def csvQuote(value, sep, quoteChar) {
    def v = value == null ? '' : value.toString()
    if (v.contains(sep) || v.contains(quoteChar) || v.contains('\n') || v.contains('\r')) {
        return quoteChar + v.replace(quoteChar, quoteChar + quoteChar) + quoteChar
    }
    return v
}

// A name_template containing '{' is used as-is; otherwise it must name a built-in preset.
def resolveTemplate(t) {
    if (t.contains('{')) return t
    def known = presets()
    if (known.containsKey(t)) return known[t]
    error "Unknown name_template preset '${t}' -- expected a literal template containing '{' or one of: ${known.keySet().join(', ')}"
}

// Every {token} in the template must be either the synthetic 'read' placeholder
// or an actual samplesheet column -- caught once, up front, rather than as a
// silent no-op substitution deep in per-row processing.
def validateTemplatePlaceholders(template, cols) {
    def allowed = (cols + ['read']) as Set
    def tokens = (template =~ /\{([^}]+)\}/).collect { it[1] }
    def unknown = tokens.findAll { !(it in allowed) }
    if (unknown) {
        error "name_template references unknown placeholder(s): ${unknown.join(', ')} -- expected one of: ${allowed.join(', ')}"
    }
}

// Placeholders come from the row's own columns (so any column is usable, not just
// a fixed sample/lane/set set), plus the one synthetic 'read' placeholder, which
// isn't a column value -- it's the label configured for whichever read_cols column
// this particular file came from.
def renderTemplate(template, row, cols, readLabel) {
    def out = template
    cols.each { c -> out = out.replace("{${c}}", row[c]?.toString() ?: '') }
    out = out.replace('{read}', readLabel)
    return out
}

// Stages one file under its rendered name. Writes directly under samplesDir
// (rather than via publishDir) so file_op's meaning is unambiguous: chaining our
// own symlink/link/copy with a second publishDir-level transformation risks
// double-hop symlink chains or silently dereferencing when the caller asked for
// a symlink. original.toRealPath() collapses Nextflow's own staging symlink so a
// 'symlink' request points straight at the true source file, not a chain through
// the task work directory.
process STAGE_FILE {
    tag "$newName"

    input:
    tuple val(rowIdx), val(col), val(newName), path(original)
    val samplesDir
    val fileOp

    output:
    tuple val(rowIdx), val(col), val("${samplesDir}/${newName}")

    script:
    def cmd = fileOp == 'copy' ? 'cp' : (fileOp == 'link' ? 'ln' : 'ln -s')
    """
    mkdir -p ${samplesDir}
    ${cmd} ${original.toRealPath()} ${samplesDir}/${newName}
    """
}

workflow SAMPLE_RENAME {
    take:
    samplesheet
    read_cols
    name_template
    file_op
    outdir
    outname
    quote

    main:
    if (!samplesheet)   error "Missing required samplesheet"
    if (!read_cols)     error "Missing required read_cols"
    if (!name_template) error "Missing required name_template"
    if (!outdir)        error "Missing required outdir"

    def sep  = detectSep(samplesheet)
    def rows = file(samplesheet, checkIfExists: true)
        .splitCsv(header: true, sep: sep, quote: quote)
    if (!rows) error "Samplesheet '${samplesheet}' has no data rows"
    def cols = rows[0].keySet() as List

    def readColPairs = splitList(read_cols).collect { pair ->
        def parts = pair.split('=', 2)
        if (parts.size() != 2) error "read_cols entries must be 'column=label' pairs, got '${pair}'"
        def (col, label) = parts
        if (!(col in cols)) error "read_cols column '${col}' not found in samplesheet header: ${cols}"
        [col, label]
    }
    if (!readColPairs) error "read_cols must specify at least one column=label pair"

    def template = resolveTemplate(name_template)
    validateTemplatePlaceholders(template, cols)

    def samplesDir     = "${outdir}/samples"
    def samplesheetDir = "${outdir}/samplesheet"

    // Stable row identity, since process completion order isn't guaranteed and we
    // need to splice staged paths back onto the correct original row later.
    def indexedRows = []
    rows.eachWithIndex { row, idx -> indexedRows << (row + [__row_idx__: idx]) }
    def rowById = indexedRows.collectEntries { row -> [(row.__row_idx__): row] }

    // Bookkeeping (row/column reasoning, template rendering, collision detection)
    // stays plain Groovy -- no file I/O happens here. Only the actual per-file
    // staging below goes through channels/a process.
    def tasks = []
    indexedRows.each { row ->
        def rowTasks = readColPairs.findResults { pair ->
            def (col, label) = pair
            def val = row[col]?.toString()?.trim()
            if (!val) return null  // blank read column for this row -- e.g. unpaired/single-end
            [rowIdx: row.__row_idx__, col: col, newName: renderTemplate(template, row, cols, label), orig: val]
        }
        if (!rowTasks) {
            def readColNames = readColPairs.collect { it[0] }.join(', ')
            error "Row ${row.__row_idx__} has no non-blank value in any of the read_cols columns: ${readColNames}"
        }
        tasks += rowTasks
    }

    def dupes = tasks.groupBy { it.newName }.findAll { k, v -> v.size() > 1 }
    if (dupes) {
        error "name_template produced duplicate filenames: ${dupes.keySet().join(', ')} -- add a distinguishing column to the samplesheet and reference it in name_template"
    }

    def renameCh = channel.fromList(tasks)
        .map { t -> tuple(t.rowIdx, t.col, t.newName, file(t.orig)) }

    STAGE_FILE(renameCh, samplesDir, file_op)

    // Rejoin staged paths back onto their row: group the (possibly several, e.g.
    // R1 and R2) staged-file completions that belong to the same row, merge them
    // into one column-update map, then splice that onto the original row so every
    // other column passes through untouched.
    def outputRows = STAGE_FILE.out
        .map { rowIdx, col, finalPath -> tuple(rowIdx, [(col): finalPath]) }
        .groupTuple()
        .map { rowIdx, updates -> tuple(rowIdx, updates.inject([:]) { acc, m -> acc + m }) }
        .map { rowIdx, updates -> rowById[rowIdx] + updates }

    def outSep = detectSep(outname)

    def samplesheetCh = outputRows
        .toSortedList { a, b -> a.__row_idx__ <=> b.__row_idx__ }
        .map { rowMaps ->
            def header = cols.collect { c -> csvQuote(c, outSep, quote) }.join(outSep)
            def body   = rowMaps.collect { r -> cols.collect { c -> csvQuote(r[c], outSep, quote) }.join(outSep) }.join('\n')
            header + '\n' + body + '\n'
        }
        .collectFile(name: outname, storeDir: samplesheetDir)

    emit:
    samplesheet = samplesheetCh
    samples     = STAGE_FILE.out
}

workflow {
    if (params.help) {
        log.info paramsHelp(command: "nextflow run main.nf --samplesheet <path> --read_cols <col=label,...> --name_template <template|preset> --outdir <dir>")
        exit 0
    }
    validateParameters()

    SAMPLE_RENAME(
        params.samplesheet,
        params.read_cols,
        params.name_template,
        params.file_op,
        params.outdir,
        params.outname,
        params.quote,
    )
}
