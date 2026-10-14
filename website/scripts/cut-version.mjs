#!/usr/bin/env node
/**
 * Cuts a new documentation version snapshot.
 *
 * Wraps `docusaurus docs:version` with the policy from VERSIONING.md, because
 * the raw command happily accepts anything and the resulting mess is only
 * visible months later:
 *
 *   - versions are MAJOR.MINOR only ("1.8"), never patch ("1.8.0")
 *   - versions are cut in ascending order
 *   - a version is never cut twice
 *
 * Note that not every release runs this. A snapshot is cut only when a release
 * changes documented behaviour — a changed default, a rename, a removal, a
 * deprecation — or when it is a major. Purely additive releases get a `_1.8+_`
 * marker in docs/ instead, which is why the versions here are expected to be
 * sparse: 1.7 then 2.0 is a healthy sequence, not a mistake. See VERSIONING.md
 * rules 1 and 2. This script cannot check that condition, so it is on you.
 *
 * Usage: npm run cut-version -- 1.8
 */
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const siteDir = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '..',
);
const versionsFile = path.join(siteDir, 'versions.json');

// Shared with docusaurus.config.ts, which uses the same window to decide what
// to build. Read rather than duplicated so a bump in one cannot leave this
// script warning about snapshots the site is still publishing.
const MAX_LIVE_VERSIONS = JSON.parse(
  fs.readFileSync(path.join(siteDir, 'versions.config.json'), 'utf8'),
).maxLiveVersions;

function fail(message) {
  console.error(`\x1b[31merror\x1b[0m ${message}`);
  process.exit(1);
}

const version = process.argv[2];

if (!version) {
  fail('missing version argument\n\n  usage: npm run cut-version -- 1.8\n');
}

if (!/^\d+\.\d+$/.test(version)) {
  fail(
    `"${version}" is not a MAJOR.MINOR version.\n\n` +
      '  Snapshots are cut per minor release only — "1.8", not "1.8.0" or "v1.8".\n' +
      '  A patch release that changes documented behaviour is edited into the\n' +
      '  existing snapshot in place. See VERSIONING.md.\n',
  );
}

const existing = fs.existsSync(versionsFile)
  ? JSON.parse(fs.readFileSync(versionsFile, 'utf8'))
  : [];

if (existing.includes(version)) {
  fail(
    `version "${version}" already exists.\n\n` +
      `  Edit versioned_docs/version-${version}/ directly for a patch-level fix.\n`,
  );
}

const [newest] = existing;
if (newest) {
  const asNumbers = (v) => v.split('.').map(Number);
  const [major, minor] = asNumbers(version);
  const [newestMajor, newestMinor] = asNumbers(newest);
  if (major < newestMajor || (major === newestMajor && minor <= newestMinor)) {
    fail(
      `version "${version}" is not newer than the current newest ("${newest}").\n`,
    );
  }
}

console.log(`Cutting docs version ${version}...`);
try {
  execFileSync('npx', ['docusaurus', 'docs:version', version], {
    cwd: siteDir,
    stdio: 'inherit',
  });
} catch {
  // Docusaurus has already printed the reason to stderr; re-dumping the
  // execFileSync error object on top of it only buries it.
  fail(`docusaurus docs:version ${version} failed (see above)`);
}

const updated = JSON.parse(fs.readFileSync(versionsFile, 'utf8'));
const previouslyLive = new Set(existing.slice(0, MAX_LIVE_VERSIONS));
const dropped = updated
  .slice(MAX_LIVE_VERSIONS)
  .filter((v) => previouslyLive.has(v));

console.log(
  `\n\x1b[32mdone\x1b[0m  versions.json is now: ${updated.join(', ')}`,
);

if (dropped.length > 0) {
  console.log(
    `\n\x1b[33mnote\x1b[0m  ${dropped.join(', ')} ${
      dropped.length === 1 ? 'is' : 'are'
    } now outside the ` +
      `${MAX_LIVE_VERSIONS}-version live window and will no longer be built.\n` +
      '      The files stay in git. To drop them for good:\n' +
      dropped
        .map(
          (v) =>
            `        git rm -r versioned_docs/version-${v} ` +
            `versioned_sidebars/version-${v}-sidebars.json`,
        )
        .join('\n') +
      '\n      then remove them from versions.json.\n',
  );
}

console.log(
  'Next: update the "Next (unreleased)" docs in docs/ for whatever lands after this release.',
);
