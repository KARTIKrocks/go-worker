# Documentation versioning

Versioned docs get unmaintainable fast if every release snapshots the whole
tree, so this site follows five rules.

## The rules

### 1. Snapshot when a release changes documented behaviour — not on every release

A snapshot exists to answer one question: *what was true before this release
changed it?* If nothing changed, the snapshot is a byte-identical copy of every
page that has to be maintained forever.

**The test:** does this release make an existing page **wrong for someone on the
previous version**? A changed default, changed semantics, a rename, a removal, a
deprecation — those get a snapshot. Purely additive releases do not.

`v0.1.0` is the worked example. It made `JobTimeout` default to no timeout,
applied it per attempt, stopped retrying panics, and made `Scheduler.Stop`
permanent — every one of those made an existing page wrong for someone on the
previous release. That earned `version-0.1`.

**Pre-1.0, expect more snapshots.** Under semver, a `0.x` minor may break the
API, and go-worker's do. Rule 1 still applies — snapshot only the minors that
change documented behaviour — but in `0.x` that is most of them.

Go makes this rule stronger than it would be elsewhere. From `v1.0.0` on, semver plus
the Go compatibility promise means additive minors leave the documented
contract intact, and a `v2` is a different import path
(`github.com/KARTIKrocks/go-worker/v2`) with its own pkg.go.dev — a genuinely
different package. **Always snapshot at a major.**

### 2. Additive changes get a version marker, not a snapshot

The one real problem a reader on an older version has is the inverse of a
breaking change: they read about something that does not exist in their version
yet, and it does not compile. A marker is the cheap fix, and a better answer — a
snapshot tells you what existed, a marker tells you what upgrading buys you.

The convention is plain Markdown, so it needs no components and survives being
copied into a snapshot:

| Situation | Write |
| --- | --- |
| New row in an API table | append `_0.2+_` to the description cell |
| New option or behaviour in prose | open the paragraph with `_Added in 0.2._` |
| New member inside a code block | trailing `// 0.2+` comment |
| Behaviour that changed | `_Changed in 0.2._` plus one line on what it was before |

Markers use `MAJOR.MINOR` — `0.2`, not `v0.2.0` — so they match snapshot names
and stay greppable. Drop a marker once it names a version older than the oldest
live snapshot; by then everyone reading has it.

### 3. Snapshots are `MAJOR.MINOR`, never patch

Versions are `0.1`, `0.2`, `1.0`. Never `0.1.0`, never `v0.1`.

A patch release (`0.1.1`) that changes documented behaviour is **edited into the
existing `versioned_docs/version-0.1/` in place**. It does not get its own
snapshot. A patch by definition does not change the contract; if it did, the
docs were already wrong, and the fix belongs in the snapshot that is wrong.

`npm run cut-version` enforces the format; it rejects anything that isn't
`MAJOR.MINOR`, already exists, or is older than the current newest.

### 4. Only the newest 4 versions are built

`maxLiveVersions` in `versions.config.json` caps how many snapshots get built
and indexed. Older ones stay in git — readable at their tag, restorable by
raising the number — but they don't cost build time or search index size.

Four is generous under rule 1. Raise it only if the reason is real support
obligations, not release count.

### 5. `docs/` is the future, not the present

| Directory | Serves | URL |
| --- | --- | --- |
| `docs/` | **Next** — unreleased, tracks `main` | `/docs/next/` |
| `versioned_docs/version-0.1/` | the current release | `/docs/` |

A reader who lands on `/docs/` sees released behaviour. Someone who wants what's
on `main` opens `/docs/next/`, which carries an "unreleased" banner.

This means **a PR that changes documented behaviour edits `docs/`**, not the
snapshot. The snapshot is frozen history.

## Release runbook

### Every release

Make sure `docs/` describes the release accurately — everything merged into
`main` since the last release should already be reflected there — and that new
APIs carry their markers. If the release number turned out different from the
one the markers name, fix the markers before tagging.

### If the release only adds

Nothing else to do. `docs/` becomes the new truth on the next deploy, the
markers tell readers on older versions what they need to upgrade to, and the
existing snapshot keeps serving `/docs/`.

That is the catch: `/docs/` serves the newest **snapshot**, so an additive
release does not reach the default URL until the next snapshot is cut. Readers
see the last release whose behaviour is fully described, and `/docs/next/`
carries everything newer. If that gap gets uncomfortably wide, cut a snapshot
even without a breaking change.

### If the release changes documented behaviour

```bash
cd website
npm run cut-version -- 0.2
npm run check
```

`versions.json`, `versioned_docs/version-0.2/`, and
`versioned_sidebars/version-0.2-sidebars.json` are created for you, `/docs/`
starts serving 0.2, and 0.1 moves into the version dropdown.

If the cut pushes a version out of the 4-version window, the script tells you
which one and prints the `git rm` to drop it for good.

### Patch releases

Never a snapshot. Edit `versioned_docs/version-<minor>/` directly, and mirror
the change into `docs/` if it still applies to `main`.

## Day-to-day

```bash
npm start          # dev server, builds Next + newest version only (fast)
npm run start:all  # dev server with every live version
npm run build      # production build, all live versions
npm run build:fast # build check, Next + newest only — what CI runs on PRs
npm run check      # lint + typecheck + fast build
```

`DOCS_FAST_BUILD=true` is what makes the first and fourth fast: it narrows the
build to the docs you're actually editing. Never set it for a production
deploy — the deploy workflow doesn't.

## Adding a page

1. Create `docs/<id>.md` with `id`, `title`, and `description` frontmatter.
2. Add the id to `sidebars.ts` under the right category.

The sidebar is explicit rather than autogenerated so ordering is a deliberate
choice and a new file can't silently rearrange the nav.

## What does not belong here

Type signatures, method sets, and struct fields. Those are generated from source
on [pkg.go.dev](https://pkg.go.dev/github.com/KARTIKrocks/go-worker) and are
always correct; hand-copying them here creates a second source of truth that
drifts.

These guides cover concepts, grouped overviews, configuration, and worked
examples — and link out for the exact signatures.
