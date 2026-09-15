---
name: code-review
description: Review a pull request for DoubleZero Edge glossary conformance. Use on every pull request. Checks prose, identifiers, CLI flags, config keys, metric names, and log fields against the vendored GLOSSARY.md in this directory.
---

# Code Review: Glossary Conformance

## Authority

`GLOSSARY.md` in this directory is the vocabulary for this repository. It is a
vendored copy of `GLOSSARY.md` in `malbeclabs/edge-feed-spec`, which is the sole
authority. The header of the vendored file records the version it carries.

A definition in the glossary overrides any local one, wherever it appears. This
repository may add a term that it alone needs. It may not redefine a term that
the glossary lists.

Read `GLOSSARY.md` before you review. Do not review from memory. The banned-word
table and the notes below it carry exceptions, and a review that misses an
exception reports a false finding.

## Scope

Check every line that the pull request adds or rewrites:

- Prose: specs, docs, READMEs, plans, doc comments, and code comments.
- The pull request title and description.
- Names that the change introduces: identifiers, type names, CLI flags, config
  keys, metric names, and log fields. The glossary binds these too.

Leave unchanged lines alone. A banned word that this change does not touch is
out of scope for this review.

Skip `.github/skills/code-review/GLOSSARY.md` entirely. That file is the
authority, not text judged against it. It lists every banned word in its tables
and explains each one, so checking it reports the vocabulary back as a wall of
findings. A pull request that only updates it is the sync workflow doing its
job.

Outside that file, a banned word survives only where the sentence is about the
word itself: naming it to say it is banned, or quoting a glossary row to explain
a replacement. A definition quoted as cover for ordinary use is still a finding,
and so is a quoted identifier, config key or metric name.

## What to flag

Flag an added or rewritten line when it does one of these:

- Uses a word from the banned-word table outside a documented exception.
- Redefines a glossary term.
- Uses a glossary term for something the `Not` column excludes. `Source ID`
  names a matching engine, so a comment that calls it a venue is a finding even
  though the words are correct English.
- Coins a synonym for a term the glossary already defines.

Do not flag these:

- An ordinary English use the glossary leaves alone. The glossary says which
  ones in its notes. `sibling module` and `path` are both correct.
- A documented exception. `ARM64`, the `ARM` vendor, `Unix epoch`,
  `source of truth`, `snapshot stream`, `delta stream`, and `Trade Flags` bit 1
  all stay as written.
- An external name that is not itself a banned word. A venue API field keeps the
  venue spelling. A new external name that is a banned word is still a finding:
  the glossary exempts the two `ARM` proper nouns by name, not every name an
  outside party happens to own, so a field called `arm` does not inherit that
  exception.

## Finding shape

Write each finding in this shape, and name the replacement term:

  **Issue**
  What word the line uses, and what the glossary requires instead.

  **Context**
  Where the word appears, and what the glossary row or note says.

  **Proposed Fix**
  The replacement, in one sentence.

Write in Simplified Technical English. Use the active voice. Name the actor.
Keep one idea in one sentence. Do not use an em dash.

Name the glossary version you reviewed against in the review summary. Take it
from the header of `GLOSSARY.md`.
