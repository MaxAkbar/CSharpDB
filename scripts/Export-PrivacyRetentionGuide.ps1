[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$body = (ConvertFrom-Markdown -Path (Join-Path $repoRoot 'docs/privacy-retention.md')).Html
$toc = ([regex]::Matches($body, '<h2 id="([^"]+)">(.+?)</h2>') | ForEach-Object {
    '<li><a href="#' + $_.Groups[1].Value + '">' + $_.Groups[2].Value + '</a></li>'
}) -join "`n"
$body = $body.Replace('<table>', '<div class="table-scroll"><table class="doc-table">').Replace('</table>', '</table></div>')
$body = $body.Replace('<pre>', '<div class="code-block"><pre>').Replace('</pre>', '</pre></div>')
$description = 'Configure reusable privacy and retention policies in CSharpDB Studio. Preview and mask selected personal fields while preserving records, relationships, and accounting values.'
$example = @'
<section class="privacy-example" aria-labelledby="example-title">
<h2 id="example-title">An example: keep the transaction, change the personal fields</h2>
<p>Illustrative values for an eligible customer and a mapped order. Each field operation is chosen explicitly.</p>
<div class="table-scroll"><table class="doc-table">
<thead><tr><th scope="col">Field</th><th scope="col">Before</th><th scope="col">After</th><th scope="col">Operation</th></tr></thead>
<tbody>
<tr><td>Customer name</td><td>Alice Example</td><td>Anonymous</td><td>Constant</td></tr>
<tr><td>Email</td><td>alice@example.test</td><td>*****@example.test</td><td>Email mask</td></tr>
<tr><td>Order shipping address</td><td>12 Example Street</td><td><code>NULL</code></td><td>Erase</td></tr>
<tr><td>Customer / order IDs</td><td>1042 / 8001</td><td>1042 / 8001</td><td>Preserve</td></tr>
<tr><td>Order amount / tax</td><td>125.50 / 10.50</td><td>125.50 / 10.50</td><td>Preserve</td></tr>
</tbody></table></div>
<p><strong>Partial masking retains information.</strong> This email preset keeps the domain. Changing selected fields does not establish that the entire person or database is unidentifiable.</p>
</section>
'@
$body = $body.Replace('<h2 id="manual-workflow">', $example + "`n" + '<h2 id="manual-workflow">')
$toc = '<li><a href="#example-title">Example</a></li>' + "`n" + $toc
$sharedCss = @'
.doc-content code{overflow-wrap:anywhere}.table-scroll{overflow-x:auto;margin:1.5rem 0}.table-scroll table{margin:0;width:100%}.privacy-example{padding:1.5rem;border:1px solid var(--border,#d7e2e5);border-radius:12px;margin:2rem 0}.privacy-example h2{margin-top:0}.code-block pre{max-width:100%;overflow-x:auto}.doc-content h2{scroll-margin-top:100px}
@media print{#site-nav,#site-footer,.doc-sidebar,.print-button,.skip-link{display:none!important}.page,.container,.doc-container,.doc-content{display:block!important;max-width:none!important;width:auto!important;margin:0!important;padding:0!important}body{background:white!important;color:#172c35!important;font-size:10pt!important}h1,h2,h3{color:#172c35!important;break-after:avoid}p,li{orphans:3;widows:3}pre{white-space:pre-wrap!important;overflow-wrap:anywhere}pre,code,table,th,td{color:#172c35!important;background:transparent!important}.privacy-example{break-inside:auto}tr{break-inside:avoid}.table-scroll{overflow:visible}a{color:inherit!important}a[href^="#"]{text-decoration:none}@page{margin:17mm}}
'@
$site = @"
<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Privacy &amp; Retention — CSharpDB</title>
<meta name="description" content="$description">
<link rel="canonical" href="https://csharpdb.com/docs/privacy-retention.html">
<meta property="og:title" content="Privacy &amp; Retention — CSharpDB">
<meta property="og:description" content="$description">
<meta property="og:url" content="https://csharpdb.com/docs/privacy-retention.html">
<meta property="og:image" content="https://csharpdb.com/images/og-banner.png">
<link rel="icon" type="image/png" href="../favicon.png">
<link rel="stylesheet" href="../css/style.css">
<link rel="preload" href="../js/csharpdb.bundle.js" as="script">
<script type="application/ld+json">{"@context":"https://schema.org","@type":"TechArticle","name":"Privacy & Retention","description":"$description","url":"https://csharpdb.com/docs/privacy-retention.html","isPartOf":{"@type":"WebSite","name":"CSharpDB","url":"https://csharpdb.com"}}</script>
<style>$sharedCss</style>
</head>
<body>
<!-- Generated from docs/privacy-retention.md by scripts/Export-PrivacyRetentionGuide.ps1. -->
<script>window.currentPage='docs';window.pagePathPrefix='../';window.pageConfig={title:'Privacy & Retention',description:'$description',keywords:'CSharpDB privacy retention PII masking anonymization Data Hygiene',canonicalPath:'docs/privacy-retention.html',ogType:'article'};</script>
<div id="site-nav"></div>
<main class="page"><div class="container doc-container">
<aside class="doc-sidebar"><nav class="doc-toc" aria-label="On this page"><h4>On This Page</h4><ul>$toc</ul></nav></aside>
<article class="doc-content">$body
<p>Related guides: <a href="admin-ui.html">Studio / Admin UI</a>, <a href="database-modes.html">Database Modes</a>, and <a href="test-data-generator.html">Test Data Generator</a>.</p>
</article></div></main>
<div id="site-footer"></div>
<script src="../js/csharpdb.bundle.js" defer></script>
</body></html>
"@
$offlineCss = @'
:root{--ink:#182f3b;--muted:#536a76;--accent:#08786e;--border:#d8e3e6;--paper:#fff;--ground:#eef3f4;color-scheme:light}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:var(--ground);color:var(--ink);font:16px/1.7 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}a{color:var(--accent);text-underline-offset:3px}a:focus-visible,button:focus-visible{outline:3px solid #bf7911;outline-offset:4px}.skip-link{position:absolute;left:1rem;top:-100px;background:white;padding:.5rem 1rem;z-index:5}.skip-link:focus{top:1rem}.masthead{padding:20px max(24px,calc((100vw - 1260px)/2));display:flex;align-items:center;justify-content:space-between;gap:1rem;border-bottom:1px solid var(--border);background:var(--paper)}.brand{font-weight:750;letter-spacing:.04em}.brand span{color:var(--muted);font-weight:400;letter-spacing:0;margin-left:12px}.print-button{background:var(--ink);color:white;border:0;border-radius:7px;padding:10px 16px;font:inherit;font-size:14px;cursor:pointer}.layout{display:grid;grid-template-columns:235px minmax(0,1fr);max-width:1260px;margin:32px auto;gap:38px;padding:0 24px}.doc-sidebar{align-self:start;position:sticky;top:24px;font-size:14px}.doc-sidebar h2{font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);margin:0 0 15px}.doc-sidebar ul{list-style:none;padding:0;margin:0}.doc-sidebar li{margin:0 0 3px}.doc-sidebar a{display:block;text-decoration:none;color:var(--muted);padding:7px 12px;border-left:2px solid var(--border)}.doc-sidebar a:hover{color:var(--accent);border-color:var(--accent);background:#e4eeed}.doc-content{min-width:0;background:var(--paper);padding:44px 48px;border:1px solid var(--border);border-radius:14px;box-shadow:0 8px 30px #18333b06}.eyebrow{color:var(--accent);font-size:12px;font-weight:750;letter-spacing:.13em;text-transform:uppercase;margin:0 0 8px}h1{font:700 clamp(2.1rem,4vw,3.1rem)/1.14 Georgia,serif;letter-spacing:-.035em;margin:0 0 22px}h1+p{font-size:18px;color:var(--muted)}h2{font-size:1.45rem;line-height:1.35;letter-spacing:-.015em;margin:42px 0 16px;border-top:1px solid var(--border);padding-top:26px}p{margin:0 0 18px}li{padding-left:4px;margin-bottom:10px}ol,ul{padding-left:24px}strong{font-weight:650}.privacy-example{background:#f3f8f7;border-top:4px solid var(--accent)}.privacy-example h2{border:0;padding:0;font-size:1.3rem}.privacy-example p{font-size:14px}.privacy-example p:last-child{margin-bottom:0}table{border-collapse:collapse;font-size:14px;text-align:left}th{color:var(--muted);font-size:12px;letter-spacing:.035em;background:#edf3f3}th,td{padding:11px 13px;border-bottom:1px solid var(--border);vertical-align:top}code{font: .87em/1.6 ui-monospace,SFMono-Regular,Consolas,monospace;background:#edf2f4;padding:2px 5px;border-radius:4px}pre{background:#152c38;color:#e7f4f3;border-radius:9px;padding:22px;font-size:13px;line-height:1.65}pre code{background:none;color:inherit;padding:0}.document-footer{margin-top:38px;padding-top:22px;border-top:1px solid var(--border);color:var(--muted);font-size:13px}@media(max-width:850px){.layout{grid-template-columns:1fr;gap:20px;margin-top:22px}.doc-sidebar{position:static}.doc-sidebar ul{display:flex;flex-wrap:wrap;gap:5px}.doc-sidebar li{margin:0}.doc-sidebar a{border:1px solid var(--border);border-radius:5px;padding:5px 9px;font-size:12px}.doc-content{padding:30px}}@media(max-width:520px){.masthead{padding:16px}.brand span{display:none}.layout{padding:0 12px}.doc-content{padding:24px 18px}.privacy-example{padding:18px 12px}h1+p{font-size:16px}.table-scroll table{min-width:490px}th,td{padding:9px 10px}}@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}}@media print{.masthead{display:none}.layout{display:block;margin:0;padding:0}.doc-content{border:0;box-shadow:none}.table-scroll table{min-width:0!important}h1{font-size:28pt}h2{font-size:16pt;margin-top:22px;padding-top:12px}.privacy-example{background:transparent}.document-footer{font-size:9pt}}
'@
$offline = @"
<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="icon" href="data:,">
<title>Privacy &amp; Retention — CSharpDB Guide</title><meta name="description" content="$description">
<style>$offlineCss`n$sharedCss</style></head><body>
<!-- Self-contained; generated from docs/privacy-retention.md. No external assets. -->
<a class="skip-link" href="#guide">Skip to guide</a>
<header class="masthead"><div class="brand">CSharpDB <span>Product guide</span></div><button type="button" class="print-button" id="print-guide">Print / Save PDF</button></header>
<div class="layout"><aside class="doc-sidebar"><nav aria-label="On this page"><h2>In this guide</h2><ul>$toc</ul></nav></aside>
<main class="doc-content" id="guide"><p class="eyebrow">Data Hygiene · Manual preview and apply</p>$body
<footer class="document-footer">CSharpDB · Privacy &amp; Retention<br>Offline edition. Source: <code>docs/privacy-retention.md</code>. Regenerate with <code>scripts/Export-PrivacyRetentionGuide.ps1</code>.</footer>
</main></div><script>document.getElementById('print-guide').addEventListener('click',function(){window.print();});</script>
</body></html>
"@
[IO.File]::WriteAllText((Join-Path $repoRoot 'www/docs/privacy-retention.html'), $site)
[IO.File]::WriteAllText((Join-Path $repoRoot 'docs/privacy-retention.html'), $offline)
Write-Host 'Generated website and self-contained Privacy & Retention HTML guides.'
