# Maintaining the public changelog

`www/changelog.html` is a concise, curated view of the release notes, not another
detailed change history. Use the existing `RELEASE_NOTES.md` for current release
details and the published GitHub Release notes for historical entries. Do not
copy raw commit lists or treat tags and planning documents as shipped features.

## During development

- Add meaningful user-facing changes to the website's **Unreleased** section.
- Leave that entry without a publication date or claimed released version.
- Link tutorials for instructions and the roadmap for future work.
- Keep release summaries short; call out data-format, type, or other upgrade
  restrictions separately. Older entries may live in the expandable archive.

## Preparing a release

1. Prepare the existing `RELEASE_NOTES.md` and package version as usual. Its first
   `## CSharpDB x.y.z` section is consumed by the existing release publisher;
   **do not put an Unreleased section before it**.
2. Review the website's candidate summary against those notes. Set
   `data-release-version="x.y.z"` on the Unreleased article, keeping
   `data-release-status="unreleased"` and no date. Do not add a publication record
   yet. A failed qualification attempt must never become a published entry.
3. Record the reviewed version and normalized notes hash in `notesReview` in
   `docs/releases/changelog-publication.json`. The hash covers the existing
   notes, normalized to LF and trimmed at both ends; it is a review reminder,
   not proof that the prose is semantically correct. The check prints the new
   hash when notes change. Review first, then update the acknowledgement.
4. Run `./scripts/Test-Documentation.ps1`. It includes the changelog check and
   already runs in CI, Pages validation, and SQL release qualification. No
   additional publisher or release workflow is required.

## After confirmed publication

1. Check the **published GitHub Release**, not the tag. Read its `published_at`
   timestamp and use the UTC calendar date. Exclude drafts and prereleases from
   this stable-release history.
2. Add `{ "version": "x.y.z", "publishedDate": "yyyy-MM-dd" }` to the publication
   record, newest version first. Update `verifiedOn` after live reconciliation.
   This file records publication facts only; full release notes remain the
   authoritative content source. History from `coverageFrom` onward must be
   complete. Do not move that boundary forward to bypass missing entries.
3. Promote the candidate article to `data-release-status="published"`, add the
   exact `<time datetime="yyyy-MM-dd">` date and `<h3>vx.y.z — Title</h3> heading,
   and link directly to `https://github.com/MaxAkbar/CSharpDB/releases/tag/vx.y.z`.
   Update the Latest release label/link, and create a fresh unversioned
   Unreleased section above it. Keep unreleased follow-on work out of the
   published summary.
4. Run these read-only checks before publishing the website update:

   ```powershell
   ./scripts/Test-Changelog.ps1 -VerifyPublishedReleases
   ./scripts/Test-ChangelogGuardrails.ps1
   ./scripts/Test-Documentation.ps1
   ```

Normal checks use the checked-in publication record and need no network or
credentials. `-VerifyPublishedReleases` reconciles that record against the public
GitHub API, including pagination, and fails on drift or network errors. It does
not create releases, change tags, update files, or trigger a deployment. Run it
after each release and during changelog maintenance; an offline check cannot
discover a release that has never been recorded locally.

## Maintaining downloads alongside the changelog

`www/downloads.html` is a static, version-pinned download guide. It works without
JavaScript or a live GitHub API call in the visitor's browser. Its release/date
must match the latest **published** entry in the changelog record, not the
working-tree package version. Candidate releases must not replace stable downloads.

After confirmed publication:

1. Review the release's actual assets and record all their names, version, UTC
   publication date, and verification date in
   `docs/releases/downloads-publication.json`. Use the public GitHub Release API;
   do not infer filenames or platform availability from build scripts alone.
2. Update the page's release badge, date, install command, versioned NuGet links,
   source tag, download links, and upgrade-note anchor together. Link every
   non-NuGet asset directly and every published package through NuGet. Update
   the visible package counts when the inventory changes.
3. Recheck platform/architecture labels, runtime prerequisites, launch instructions,
   optional adapter restrictions, and checksum scope. Keep each checksum list
   beside its matching archive family. The package manifest is not a native
   library checksum file. Do not claim new platforms based on source-build support.
4. Review source-only notices, including the Data Modeler tutorial and unpublished
   Node wrapper. Only advertise an npm install command after verifying both the
   public registry entry and that it is the intended CSharpDB package. When the
   wrapper changes, recheck its own version and local tarball instructions.
5. Run the following read-only checks, then browser-check the page with all
   expandable sections open on mobile and desktop, in both themes:

   ```powershell
   ./scripts/Test-Downloads.ps1 -VerifyPublishedDownloads
   ./scripts/Test-DownloadsGuardrails.ps1
   ./scripts/Test-Documentation.ps1
   ```

The normal documentation check validates the page against the checked-in asset
inventory, including pinned URLs, package coverage, platform-card targets, and
changelog agreement. The optional live check compares the entire inventory and
asset URLs with GitHub's latest stable release and verifies each package version
on NuGet. It fails on drift or network errors. It does not download installers,
run applications, publish anything, or automatically update the inventory.

Offline checks cannot detect an unrecorded new release or a newly published npm
package. Reconcile live after a release and during download-page maintenance.
No new publishing workflow is required.

## Historical corrections

The previous website skipped released versions and used approximate milestone
dates. The current page lists verified releases from v3.8.0 onward, with older
published versions linked in an archive. v2.9.0, v4.5.0, v4.6.0, and v4.6.1 are
not listed as shipped releases. v3.9.1's published notes reuse the v3.9.0 heading,
so the page links that release without inventing a separate patch feature delta.
