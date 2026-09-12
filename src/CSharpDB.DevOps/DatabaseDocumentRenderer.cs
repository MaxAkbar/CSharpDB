using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace CSharpDB.DevOps;

/// <summary>Portable, dependency-free renderers for the same immutable dictionary.</summary>
public static class DatabaseDocumentRenderer
{
    public static string Anchor(string objectId) => "object-" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(objectId))).ToLowerInvariant();
    private static string H(string? text) => WebUtility.HtmlEncode(text ?? "");
    private static string Description(string? text) => string.IsNullOrWhiteSpace(text) ? "No description provided." : text;
    private static string Link(DatabaseDocumentLink link) => link.ObjectId is null ? H(link.Label) + " (unavailable)" : $"<a href=\"#{Anchor(link.ObjectId)}\">{H(link.Label)}</a>";

    public static string RenderHtml(DatabaseDocument document)
    {
        var html = new StringBuilder("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>");
        html.Append(H(document.DisplayName)).Append(" — Data dictionary</title><style>").Append(Styles).Append("</style></head><body><header><p class=\"eyebrow\">CSharpDB · Database Documenter</p><h1>")
            .Append(H(document.DisplayName)).Append(" — Data dictionary</h1><p>Captured ").Append(H(document.CapturedUtc.ToString("u", CultureInfo.InvariantCulture)))
            .Append(" · ").Append(document.Entries.Count).Append(" objects</p>");
        if (!document.IsComplete)
        {
            html.Append("<aside class=\"notice\"><strong>Incomplete documentation</strong><ul>");
            foreach (string diagnostic in document.Diagnostics) html.Append("<li>").Append(H(diagnostic)).Append("</li>");
            html.Append("</ul></aside>");
        }
        html.Append("<div id=\"search-controls\" hidden><label>Search dictionary <input id=\"dictionary-search\" type=\"search\" placeholder=\"Names, descriptions, SQL…\"></label><label>Object type <select id=\"dictionary-kind\"><option value=\"\">All types</option>");
        foreach (string kind in document.Entries.Select(e => e.Kind).Distinct()) html.Append("<option>").Append(H(kind)).Append("</option>");
        html.Append("</select></label><button id=\"dictionary-reset\" type=\"button\">Clear filters</button><span id=\"dictionary-count\" role=\"status\"></span></div><noscript><p>All objects are shown. Use your browser’s Find command to search.</p></noscript></header><div class=\"dictionary-layout\"><nav aria-label=\"Table of contents\"><h2>Contents</h2><ul>");
        foreach (var entry in document.Entries) html.Append("<li data-entry=\"").Append(Anchor(entry.Id)).Append("\"><a href=\"#").Append(Anchor(entry.Id)).Append("\">").Append(H(Qualified(entry))).Append("</a> <small>").Append(H(entry.Kind)).Append("</small></li>");
        html.Append("</ul></nav><main>");
        if (document.Entries.Count == 0) html.Append("<p>No database objects to document.</p>");
        html.Append("<p id=\"dictionary-empty\" hidden>No objects match these filters.</p>");
        foreach (var entry in document.Entries) html.Append(RenderEntryHtml(entry));
        return html.Append("</main></div><script>").Append(SearchScript).Append("</script></body></html>").ToString();
    }

    public static string RenderEntryHtml(DatabaseDocumentEntry entry)
    {
        var html = new StringBuilder($"<article class=\"document-entry\" id=\"{Anchor(entry.Id)}\" data-kind=\"{H(entry.Kind)}\"><p class=\"eyebrow\">{H(entry.Kind)}</p><h2>{H(Qualified(entry))}</h2>");
        html.Append("<p class=\"description\">").Append(H(Description(entry.Description))).Append("</p>");
        if (entry.NeedsReview) html.Append("<p class=\"notice\">The saved description needs review after a definition change and has not been applied.</p>");
        if (entry.Facts.Count > 0)
        {
            html.Append("<dl>");
            foreach (var fact in entry.Facts) html.Append("<dt>").Append(H(fact.Name)).Append("</dt><dd>").Append(H(fact.Value)).Append("</dd>");
            html.Append("</dl>");
        }
        if (entry.Columns.Count > 0)
        {
            html.Append("<h3>Columns</h3>");
            HtmlTable(html, ["Column", "Type", "Nullable", "Attributes", "Default", "Description"], entry.Columns.Select(c => new[] { Link(c.Link), H(c.Type), c.Nullable ? "Yes" : "No", H(c.Attributes), H(c.DefaultSql), H(Description(c.Description)) }));
        }
        if (entry.Parameters.Count > 0)
        {
            html.Append("<h3>Parameters</h3>");
            HtmlTable(html, ["Parameter", "Type", "Required", "Default", "Description"], entry.Parameters.Select(p => new[] { H(p.Name), H(p.Type), p.Required ? "Yes" : "No", H(p.Default), H(Description(p.Description)) }));
        }
        foreach (var group in RelationshipGroups(entry))
        {
            html.Append("<h3>").Append(group.Title).Append("</h3>");
            foreach (var relation in group.Items)
            {
                html.Append("<section class=\"relationship\"><h4>").Append(Link(new(relation.Name, relation.Id))).Append("</h4><p>").Append(Link(relation.ChildTable)).Append(" → ").Append(Link(relation.ParentTable)).Append("</p>");
                HtmlTable(html, ["Child column", "Referenced column"], relation.Columns.Select(p => new[] { Link(p.Child), Link(p.Parent) }));
                html.Append("<p>ON DELETE ").Append(H(relation.OnDelete)).Append(" · ON UPDATE ").Append(H(relation.OnUpdate)).Append("</p></section>");
            }
        }
        if (entry.RelatedObjects.Count > 0)
        {
            html.Append("<h3>Related objects and constraints</h3><ul>");
            foreach (var link in entry.RelatedObjects) html.Append("<li>").Append(Link(link)).Append("</li>");
            html.Append("</ul>");
        }
        html.Append("<h3>Definition</h3><p class=\"definition-label\">").Append(H(entry.DefinitionLabel)).Append("</p><pre><code>").Append(H(entry.Definition)).Append("</code></pre></article>");
        return html.ToString();
    }

    private static void HtmlTable(StringBuilder html, string[] headers, IEnumerable<string[]> rows)
    {
        html.Append("<div class=\"table-scroll\"><table><thead><tr>");
        foreach (string heading in headers) html.Append("<th scope=\"col\">").Append(H(heading)).Append("</th>");
        html.Append("</tr></thead><tbody>");
        foreach (var row in rows)
        {
            html.Append("<tr>");
            foreach (string cell in row) html.Append("<td>").Append(cell).Append("</td>");
            html.Append("</tr>");
        }
        html.Append("</tbody></table></div>");
    }

    public static string RenderMarkdown(DatabaseDocument document)
    {
        var md = new StringBuilder("# ").Append(M(document.DisplayName)).Append(" — Data dictionary\n\nCaptured ")
            .Append(document.CapturedUtc.ToString("u", CultureInfo.InvariantCulture)).Append(" · ").Append(document.Entries.Count).Append(" objects\n\n");
        if (!document.IsComplete)
        {
            md.Append("**Incomplete documentation**\n\n");
            foreach (string diagnostic in document.Diagnostics) md.Append("- ").Append(M(diagnostic)).Append('\n');
            md.Append('\n');
        }
        md.Append("## Contents\n\n");
        foreach (var entry in document.Entries) md.Append("- ").Append(ML(new(Qualified(entry), entry.Id))).Append(" — ").Append(M(entry.Kind)).Append('\n');
        if (document.Entries.Count == 0) md.Append("No database objects to document.\n");
        foreach (var entry in document.Entries)
        {
            md.Append("\n<a id=\"").Append(Anchor(entry.Id)).Append("\"></a>\n\n## ").Append(M(Qualified(entry))).Append("\n\n").Append(M(entry.Kind)).Append("\n\n").Append(M(Description(entry.Description))).Append("\n\n");
            if (entry.NeedsReview) md.Append("**The saved description needs review after a definition change and has not been applied.**\n\n");
            foreach (var fact in entry.Facts) md.Append("- **").Append(M(fact.Name)).Append(":** ").Append(M(fact.Value)).Append('\n');
            if (entry.Facts.Count > 0) md.Append('\n');
            if (entry.Columns.Count > 0)
            {
                md.Append("### Columns\n\n");
                MarkdownTable(md, ["Column", "Type", "Nullable", "Attributes", "Default", "Description"], entry.Columns.Select(c => new[] { ML(c.Link), M(c.Type), c.Nullable ? "Yes" : "No", M(c.Attributes), M(c.DefaultSql), M(Description(c.Description)) }));
            }
            if (entry.Parameters.Count > 0)
            {
                md.Append("### Parameters\n\n");
                MarkdownTable(md, ["Parameter", "Type", "Required", "Default", "Description"], entry.Parameters.Select(p => new[] { M(p.Name), M(p.Type), p.Required ? "Yes" : "No", M(p.Default), M(Description(p.Description)) }));
            }
            foreach (var group in RelationshipGroups(entry))
            {
                md.Append("### ").Append(group.Title).Append("\n\n");
                foreach (var relation in group.Items)
                {
                    md.Append("#### ").Append(ML(new(relation.Name, relation.Id))).Append("\n\n").Append(ML(relation.ChildTable)).Append(" → ").Append(ML(relation.ParentTable)).Append("\n\n");
                    MarkdownTable(md, ["Child column", "Referenced column"], relation.Columns.Select(p => new[] { ML(p.Child), ML(p.Parent) }));
                    md.Append("ON DELETE ").Append(M(relation.OnDelete)).Append(" · ON UPDATE ").Append(M(relation.OnUpdate)).Append("\n\n");
                }
            }
            if (entry.RelatedObjects.Count > 0)
            {
                md.Append("### Related objects and constraints\n\n");
                foreach (var link in entry.RelatedObjects) md.Append("- ").Append(ML(link)).Append('\n');
                md.Append('\n');
            }
            string fence = new((char)96, Math.Max(3, Regex.Matches(entry.Definition, "\u0060+").Select(m => m.Length + 1).DefaultIfEmpty(3).Max()));
            md.Append("### Definition\n\n").Append(M(entry.DefinitionLabel)).Append("\n\n").Append(fence).Append("sql\n").Append(entry.Definition).Append('\n').Append(fence).Append('\n');
        }
        return md.ToString();
    }

    private static void MarkdownTable(StringBuilder md, string[] headers, IEnumerable<string[]> rows)
    {
        md.Append("| ").AppendJoin(" | ", headers).Append(" |\n| ").AppendJoin(" | ", headers.Select(_ => "---")).Append(" |\n");
        foreach (var row in rows) md.Append("| ").AppendJoin(" | ", row).Append(" |\n");
        md.Append('\n');
    }
    private static string M(string? text)
    {
        var escaped = new StringBuilder();
        foreach (char c in (text ?? "").Replace("\r\n", "\n", StringComparison.Ordinal).Replace('\r', '\n'))
        {
            if (c == '\n') escaped.Append("<br>");
            else if (c == '&') escaped.Append("&amp;");
            else if (c == '<') escaped.Append("&lt;");
            else if (c == '>') escaped.Append("&gt;");
            else { if ("\\\u0060*_{}[]()#+-.!|".Contains(c)) escaped.Append('\\'); escaped.Append(c); }
        }
        return escaped.ToString();
    }
    private static string ML(DatabaseDocumentLink link) => link.ObjectId is null ? M(link.Label) + " (unavailable)" : $"[{M(link.Label)}](#{Anchor(link.ObjectId)})";
    private static string Qualified(DatabaseDocumentEntry entry) => entry.OwnerName is null ? entry.Name : entry.OwnerName + "." + entry.Name;
    private static IEnumerable<(string Title, IEnumerable<DatabaseDocumentRelationship> Items)> RelationshipGroups(DatabaseDocumentEntry entry)
    {
        if (entry.Kind == "Table")
        {
            var outgoing = entry.Relationships.Where(r => r.ChildTable.ObjectId == entry.Id).ToArray();
            var incoming = entry.Relationships.Where(r => r.ParentTable.ObjectId == entry.Id).ToArray();
            if (outgoing.Length > 0) yield return ("Outgoing relationships", outgoing);
            if (incoming.Length > 0) yield return ("Incoming relationships", incoming);
        }
        else if (entry.Relationships.Count > 0) yield return ("Relationships", entry.Relationships);
    }

    private const string Styles = """
        :root{color-scheme:light;font-family:system-ui,sans-serif;color:#17283a;background:#f5f7fa}*{box-sizing:border-box}body{margin:0}header{padding:2rem 3rem;background:#fff;border-bottom:1px solid #d9e2eb}h1{font-size:2rem;margin:.4rem 0}h2{overflow-wrap:anywhere}h3{margin-top:1.6rem}p,li{line-height:1.6}a{color:#135caf;overflow-wrap:anywhere}small,.definition-label{color:#536578}.eyebrow{text-transform:uppercase;letter-spacing:.09em;font-size:.75rem;color:#536578}.dictionary-layout{display:grid;grid-template-columns:280px minmax(0,1fr);gap:2rem;padding:2rem 3rem}nav{max-height:85vh;position:sticky;top:1rem;overflow:auto}nav ul{list-style:none;padding:0}nav li{margin-bottom:.65rem}nav small{display:block}main{min-width:0}.document-entry{background:#fff;border:1px solid #d9e2eb;border-radius:10px;padding:1.5rem;margin-bottom:1.5rem;scroll-margin-top:1rem}.description{white-space:pre-wrap;overflow-wrap:anywhere}.notice{padding:1rem;border-left:4px solid #c88a24;background:#fff5df}table{border-collapse:collapse;width:100%;font-size:.9rem}th,td{text-align:left;border-bottom:1px solid #d9e2eb;padding:.65rem;vertical-align:top;white-space:pre-wrap;overflow-wrap:anywhere}th{background:#eff3f7}.table-scroll,pre{overflow:auto}pre{padding:1rem;background:#142235;color:#e4edf6;border-radius:6px}dl{display:grid;grid-template-columns:max-content 1fr;gap:.4rem 1rem}dt{font-weight:600}dd{margin:0;overflow-wrap:anywhere}#search-controls:not([hidden]){display:flex;align-items:end;gap:1rem;flex-wrap:wrap;margin-top:1rem}label{display:grid;gap:.35rem}input,select,button{font:inherit;padding:.6rem;border:1px solid #9cacbd;border-radius:5px}input{min-width:280px}button{cursor:pointer;background:#eff3f7}[hidden]{display:none!important}@media(max-width:760px){header,.dictionary-layout{padding:1rem}.dictionary-layout{grid-template-columns:1fr}nav{position:static;max-height:220px}}@media print{nav,#search-controls{display:none!important}.dictionary-layout{display:block;padding:0}.document-entry{break-inside:avoid}pre{white-space:pre-wrap}}
        """;
    private const string SearchScript = """
        (()=>{'use strict';const search=document.getElementById('dictionary-search'),kind=document.getElementById('dictionary-kind'),entries=[...document.querySelectorAll('.document-entry')],links=[...document.querySelectorAll('nav li[data-entry]')],count=document.getElementById('dictionary-count');const index=entries.map(e=>({e,text:e.textContent.toLowerCase()}));let timer;function filter(){const q=search.value.toLowerCase(),k=kind.value;let n=0;const visible=new Set();for(const item of index){const show=(!k||item.e.dataset.kind===k)&&(!q||item.text.includes(q));item.e.hidden=!show;if(show){n++;visible.add(item.e.id)}}for(const link of links)link.hidden=!visible.has(link.dataset.entry);count.textContent=n+' of '+entries.length+' objects';document.getElementById('dictionary-empty').hidden=n!==0}function clear(){clearTimeout(timer);search.value='';kind.value='';filter()}search.addEventListener('input',()=>{clearTimeout(timer);timer=setTimeout(filter,120)});kind.addEventListener('change',filter);document.getElementById('dictionary-reset').addEventListener('click',clear);document.addEventListener('click',event=>{const a=event.target.closest('a[href^="#object-"]');if(a){const target=document.getElementById(a.getAttribute('href').slice(1));if(target&&target.hidden)clear()}});function revealHash(){const target=document.getElementById(location.hash.slice(1));if(target){if(target.hidden)clear();target.scrollIntoView()}}window.addEventListener('hashchange',revealHash);document.getElementById('search-controls').hidden=false;filter();revealHash()})();
        """;
}
