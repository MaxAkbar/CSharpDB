#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# fix-nuget-readme-links.sh
#
# Rewrites relative markdown links in README files to absolute URLs so they
# resolve correctly when displayed on NuGet.org.
#
# - Links resolving to docs/ are rewritten to https://csharpdb.com/docs/...
# - All other links are rewritten to GitHub blob URLs.
#
# Usage:
#   fix-nuget-readme-links.sh <git-ref> <readme>...
#
# Example:
#   fix-nuget-readme-links.sh v2.0.1 src/CSharpDB.Storage/README.md
#   fix-nuget-readme-links.sh main   src/*/README.md
# ---------------------------------------------------------------------------
set -euo pipefail

ref="${1:?Usage: fix-nuget-readme-links.sh <git-ref> <readme>...}"
shift

github_url="https://github.com/MaxAkbar/CSharpDB/blob/${ref}"
site_url="https://csharpdb.com"
repo_root="$(cd "$(git rev-parse --show-toplevel)" && pwd)"

for readme in "$@"; do
  [[ -f "$readme" ]] || continue

  abs_dir="$(cd "$(dirname "$readme")" && pwd)"
  rel_dir="${abs_dir#"$repo_root"/}"

  perl -i -pe '
    BEGIN {
      $dir        = "'"$rel_dir"'";
      $github     = "'"$github_url"'";
      $site       = "'"$site_url"'";

      # Mapping from docs/ markdown paths to www HTML paths
      %docs_map = (
        "docs/cli.md"                                    => "docs/cli.html",
        "docs/faq.md"                                    => "docs/faq.html",
        "docs/internals.md"                              => "docs/internals.html",
        "docs/rest-api.md"                               => "docs/rest-api.html",
        "docs/mcp-server.md"                             => "docs/mcp-server.html",
        "docs/storage-inspector.md"                      => "docs/storage-inspector.html",
        "docs/architecture.md"                           => "docs/architecture.html",
        "docs/getting-started.md"                        => "getting-started.html",
        "docs/roadmap.md"                                => "roadmap.html",
        "docs/collation-support/README.md"               => "docs/collation-support.html",
        "docs/collection-indexing/README.md"              => "docs/collection-indexing.html",
        "docs/database-encryption/README.md"              => "docs/database-encryption.html",
        "docs/deployment/README.md"                       => "docs/deployment.html",
        "docs/low-latency-durable-writes/README.md"       => "docs/low-latency-writes.html",
        "docs/pub-sub-events/README.md"                   => "docs/pub-sub-events.html",
        "docs/sql-batched-row-transport/README.md"         => "docs/sql-batched-transport.html",
        "docs/storage/README.md"                          => "docs/storage-engine.html",
        "docs/user-defined-functions/README.md"            => "docs/user-defined-functions.html",
        "docs/migrations/core-to-primitives.md"            => "docs/migrations.html",
        "docs/tutorials/README.md"                         => "docs/tutorials/index.html",
        "docs/tutorials/native-ffi/README.md"              => "docs/tutorials/native-ffi.html",
        "docs/tutorials/native-ffi/javascript/README.md"   => "docs/tutorials/native-ffi-javascript.html",
        "docs/tutorials/native-ffi/python/README.md"       => "docs/tutorials/native-ffi-python.html",
        "docs/tutorials/storage/README.md"                 => "docs/tutorials/storage.html",
        "docs/tutorials/storage/architecture.md"           => "docs/architecture.html",
        "docs/tutorials/storage/extensibility.md"          => "docs/tutorials/storage-extensibility.html",
        "docs/tutorials/storage/examples/README.md"        => "docs/tutorials/storage-examples.html",
      );
    }

    s{
      ( !?\[ [^\]]* \] )                       # $1 — [text] or ![alt]
      \(                                        # opening paren
        (?! https?:// | \# | mailto: )          # skip absolute URLs and anchors
        ( [^)\s\#]+ )                           # $2 — relative path
        ( \# [^)]* )?                           # $3 — optional #anchor
      \)                                        # closing paren
    }{
      my ($bracket, $path, $anchor) = ($1, $2, $3 // "");

      # Resolve relative path segments against the readme directory
      my @parts = split m{/}, "$dir/$path";
      my @resolved;
      for (@parts) {
        if    ($_ eq "..")              { pop @resolved }
        elsif ($_ ne "." && $_ ne "")   { push @resolved, $_ }
      }
      my $full = join("/", @resolved);

      # Check if this is a docs/ path with a known HTML mapping
      if (exists $docs_map{$full}) {
        "$bracket($site/$docs_map{$full}$anchor)"
      } else {
        "$bracket($github/$full$anchor)"
      }
    }gxe;
  ' "$readme"

  echo "  fixed: $readme"
done
