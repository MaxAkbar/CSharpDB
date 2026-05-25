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

      # Mapping from legacy docs/ markdown paths to www HTML paths
      %docs_map = (
        "docs/cli.md"                                    => "docs/cli.html",
        "docs/faq.md"                                    => "docs/faq.html",
        "docs/internals.md"                              => "docs/internals.html",
        "docs/rest-api.md"                               => "docs/rest-api.html",
        "docs/mcp-server.md"                             => "docs/mcp-server.html",
        "docs/storage-inspector.md"                      => "docs/storage-inspector.html",
        "docs/architecture.md"                           => "architecture.html",
        "docs/getting-started.md"                        => "docs/getting-started.html",
        "docs/roadmap.md"                                => "roadmap.html",
        "docs/ado-ef-storage-tuning/README.md"           => "docs/ado-ef-storage-tuning.html",
        "docs/admin-collections-ui/README.md"             => "docs/admin-collections.html",
        "docs/admin-forms-access-parity/form-control-extensibility.md" => "docs/form-control-extensibility.html",
        "docs/configuration.md"                          => "docs/configuration.html",
        "docs/database-devops-toolkit/README.md"          => "docs/database-devops.html",
        "docs/database-encryption/README.md"              => "docs/database-encryption.html",
        "docs/deployment/README.md"                       => "docs/deployment.html",
        "docs/low-latency-durable-writes/README.md"       => "docs/low-latency-writes.html",
        "docs/pub-sub-events/README.md"                   => "docs/pub-sub-events.html",
        "docs/storage/README.md"                          => "docs/storage-engine.html",
        "docs/user-defined-functions/README.md"            => "docs/trusted-csharp-functions.html",
        "docs/trusted-csharp-functions/README.md"          => "docs/trusted-csharp-functions.html",
        "docs/trusted-csharp-functions/access-style-macro-actions.md" => "docs/access-style-macro-actions.html",
        "docs/trusted-csharp-functions/validation-rules.md" => "docs/trusted-validation-rules.html",
        "docs/query-and-durable-write-performance/csharpdb-vs-sqlite-performance-guide.md" => "docs/csharpdb-vs-sqlite-performance-guide.html",
        "docs/query-and-durable-write-performance/csharpdb-vs-sqlite-benchmarking-blog.md" => "blog/csharpdb-vs-sqlite-benchmarking-reference.html",
        "docs/performance.md"                             => "docs/performance-reference.html",
        "docs/query-execution-pipeline.md"                 => "docs/query-execution-pipeline.html",
        "docs/sql-reference.md"                            => "docs/sql-reference.html",
        "docs/tutorials/README.md"                         => "docs/tutorials/index.html",
        "docs/tutorials/fulfillment-ops-admin-automation.md" => "docs/tutorials/fulfillment-ops-admin-automation.html",
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
