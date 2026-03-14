#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# fix-nuget-readme-links.sh
#
# Rewrites relative markdown links in README files to absolute GitHub blob
# URLs so they resolve correctly when displayed on NuGet.org.
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

base_url="https://github.com/MaxAkbar/CSharpDB/blob/${ref}"
repo_root="$(cd "$(git rev-parse --show-toplevel)" && pwd)"

for readme in "$@"; do
  [[ -f "$readme" ]] || continue

  abs_dir="$(cd "$(dirname "$readme")" && pwd)"
  rel_dir="${abs_dir#"$repo_root"/}"

  perl -i -pe '
    BEGIN {
      $dir  = "'"$rel_dir"'";
      $base = "'"$base_url"'";
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

      "$bracket($base/$full$anchor)"
    }gxe;
  ' "$readme"

  echo "  fixed: $readme"
done
