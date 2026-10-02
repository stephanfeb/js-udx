#!/usr/bin/env node
// Copies the runnable programs in examples/ into the docs in doc/, so every
// code sample on the site is one that `examples/examples.test.ts` runs.
//
// In a doc, a marker pair owns the code block between it:
//
//   <!-- example: examples/echo.ts#server -->
//   ```ts
//   ...replaced on every run...
//   ```
//   <!-- /example -->
//
// Without `#name` the whole file is embedded; with it, the lines between
// `// #region name` and `// #endregion name`. Region marker lines are never
// embedded.
//
// Usage: node tools/embed-examples.mjs [--check]
//   --check  change nothing; exit 1 if a doc is out of date.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const DOCS = path.join(ROOT, 'doc')
const BLOCK = /(<!-- example: ([^#\s]+)(?:#([\w-]+))? -->\n)[\s\S]*?(<!-- \/example -->)/g
const REGION_LINE = /^\s*\/\/ #(?:end)?region\b/

/** The embeddable text of `file`, or of its region `name`, dedented. */
function excerpt (file, name) {
  const lines = fs.readFileSync(path.join(ROOT, file), 'utf8').replace(/\r\n/g, '\n').split('\n')
  let body = lines
  if (name !== undefined) {
    const start = lines.findIndex(l => l.trim() === `// #region ${name}`)
    const end = lines.findIndex(l => l.trim() === `// #endregion ${name}`)
    if (start === -1 || end < start) throw new Error(`${file}: no region "${name}"`)
    body = lines.slice(start + 1, end)
  }
  body = body.filter(l => !REGION_LINE.test(l))
  while (body.length > 0 && body[body.length - 1].trim() === '') body.pop()
  const indent = Math.min(...body.filter(l => l.trim() !== '').map(l => l.match(/^ */)[0].length))
  return body.map(l => l.slice(indent)).join('\n')
}

/** Rewrites every example block in `markdown`. */
export function embed (markdown) {
  return markdown.replace(BLOCK, (_whole, open, file, name, close) =>
    `${open}\`\`\`ts\n${excerpt(file, name)}\n\`\`\`\n${close}`)
}

/** Docs whose example blocks differ from the examples, after rewriting them unless `check`. */
export function embedAll ({ check = false } = {}) {
  const stale = []
  for (const name of fs.readdirSync(DOCS).filter(f => f.endsWith('.md'))) {
    const file = path.join(DOCS, name)
    const before = fs.readFileSync(file, 'utf8')
    const after = embed(before)
    if (after === before) continue
    stale.push(path.relative(ROOT, file))
    if (!check) fs.writeFileSync(file, after)
  }
  return stale
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const check = process.argv.includes('--check')
  const stale = embedAll({ check })
  if (check && stale.length > 0) {
    process.stderr.write(`out of date, run npm run docs: ${stale.join(', ')}\n`)
    process.exit(1)
  }
  if (!check) process.stdout.write(`embed-examples: updated ${stale.length} docs\n`)
}
