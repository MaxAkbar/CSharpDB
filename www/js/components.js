// ═══════════════════════════════════════════════════════════
// CSharpDB Website — Shared Components
// ═══════════════════════════════════════════════════════════
// Eliminates per-page duplication by generating:
//   • <head> meta / SEO / OpenGraph / Twitter tags
//   • Navigation bar
//   • Footer
//   • Code-block headers (dots + filename)
//
// Each page only needs to declare:
//   window.currentPage   = 'docs';           // nav highlight
//   window.pagePathPrefix = '../';            // asset prefix (omit for root)
//   window.pageConfig    = { title, description, keywords, canonicalPath, ogType, jsonLd };
//
// ═══════════════════════════════════════════════════════════

(function () {
    'use strict';

    const prefix = window.pagePathPrefix || '';
    const BASE_URL = 'https://csharpdb.com/';

    // ── SVG icons ───────────────────────────────────────────

    const logoSvg = `<img src="${prefix}images/icon3.png" alt="CSharpDB" width="28" height="28">`;

    const sunSvg = `<svg class="icon-sun" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="5"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/></svg>`;
    const moonSvg = `<svg class="icon-moon" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>`;

    // ── <head> meta tags ────────────────────────────────────

    function renderMeta() {
        const cfg = window.pageConfig;
        if (!cfg) return;

        const title = cfg.title
            ? (cfg.title.includes('CSharpDB') ? cfg.title : cfg.title + ' \u2014 CSharpDB')
            : 'CSharpDB';
        const desc = cfg.description || '';
        const keywords = cfg.keywords || '';
        const canonical = BASE_URL + (cfg.canonicalPath || '');
        const ogType = cfg.ogType || 'website';
        const ogImage = BASE_URL + 'images/og-banner.png';

        document.title = title;

        const metas = {
            'description':         desc,
            'keywords':            keywords,
            'robots':              'index, follow',
        };

        const ogTags = {
            'og:title':       title,
            'og:description': desc,
            'og:type':        ogType,
            'og:url':         canonical,
            'og:site_name':   'CSharpDB',
            'og:image':       ogImage,
        };

        const twitterTags = {
            'twitter:card':        'summary_large_image',
            'twitter:title':       title,
            'twitter:description': desc,
        };

        const head = document.head;

        // Standard meta (name=...)
        for (const [name, content] of Object.entries(metas)) {
            setOrCreateMeta(head, 'name', name, content);
        }

        // OpenGraph (property=...)
        for (const [prop, content] of Object.entries(ogTags)) {
            setOrCreateMeta(head, 'property', prop, content);
        }

        // Twitter (name=...)
        for (const [name, content] of Object.entries(twitterTags)) {
            setOrCreateMeta(head, 'name', name, content);
        }

        // Canonical link
        let link = head.querySelector('link[rel="canonical"]');
        if (!link) {
            link = document.createElement('link');
            link.rel = 'canonical';
            head.appendChild(link);
        }
        link.href = canonical;

        // JSON-LD structured data (optional)
        if (cfg.jsonLd) {
            const script = document.createElement('script');
            script.type = 'application/ld+json';
            script.textContent = JSON.stringify(cfg.jsonLd);
            head.appendChild(script);
        }
    }

    function setOrCreateMeta(head, attr, value, content) {
        let el = head.querySelector(`meta[${attr}="${value}"]`);
        if (!el) {
            el = document.createElement('meta');
            el.setAttribute(attr, value);
            head.appendChild(el);
        }
        el.setAttribute('content', content);
    }

    // ── Navigation ──────────────────────────────────────────

    function navLink(href, id, label) {
        const current = window.currentPage || 'home';
        const cls = current === id ? 'nav-link active' : 'nav-link';
        return `<a href="${prefix}${href}" class="${cls}">${label}</a>`;
    }

    function renderNav() {
        const el = document.getElementById('site-nav');
        if (!el) return;
        el.innerHTML = `
        <nav class="navbar" id="navbar">
            <div class="nav-container">
                <a href="${prefix}index.html" class="nav-logo">
                    <span class="logo-icon">${logoSvg}</span>
                    <span class="logo-text">CSharpDB</span>
                </a>
                <div class="nav-links" id="navLinks">
                    ${navLink('index.html', 'home', 'Home')}
                    ${navLink('getting-started.html', 'getting-started', 'Getting Started')}
                    ${navLink('docs/index.html', 'docs', 'Docs')}
                    ${navLink('architecture.html', 'architecture', 'Architecture')}
                    ${navLink('benchmarks.html', 'benchmarks', 'Benchmarks')}
                    ${navLink('performance.html', 'performance', 'Performance')}
                    ${navLink('api-reference.html', 'api-reference', 'API Reference')}
                    ${navLink('roadmap.html', 'roadmap', 'Roadmap')}
                </div>
                <div class="nav-actions">
                    <button class="theme-toggle" id="themeToggle" title="Toggle theme">
                        ${sunSvg}${moonSvg}
                    </button>
                    <a href="https://github.com/MaxAkbar/CSharpDB" class="btn btn-outline btn-sm" target="_blank">GitHub</a>
                    <button class="mobile-toggle" id="mobileToggle" aria-label="Toggle menu">
                        <span></span><span></span><span></span>
                    </button>
                </div>
            </div>
        </nav>`;
    }

    // ── Footer ──────────────────────────────────────────────

    function renderFooter() {
        const el = document.getElementById('site-footer');
        if (!el) return;
        el.innerHTML = `
        <footer class="site-footer">
            <div class="container">
                <div class="footer-grid">
                    <div class="footer-brand">
                        <span class="logo-text">CSharpDB</span>
                        <p>A zero-dependency embedded database engine for .NET with full SQL, Collections API, and ETL pipelines.</p>
                    </div>
                    <div class="footer-col">
                        <h4>Documentation</h4>
                        <a href="${prefix}getting-started.html">Getting Started</a>
                        <a href="${prefix}docs/index.html">Documentation</a>
                        <a href="${prefix}architecture.html">Architecture</a>
                        <a href="${prefix}api-reference.html">API Reference</a>
                        <a href="${prefix}performance.html">Performance Guide</a>
                        <a href="${prefix}roadmap.html">Roadmap</a>
                    </div>
                    <div class="footer-col">
                        <h4>Features</h4>
                        <a href="${prefix}docs/sql.html">SQL Reference</a>
                        <a href="${prefix}docs/collections.html">Collections API</a>
                        <a href="${prefix}docs/pipelines.html">ETL Pipelines</a>
                        <a href="${prefix}docs/ecosystem.html">Tools & Ecosystem</a>
                    </div>
                </div>
                <div class="footer-bottom">
                    <p>Built with .NET 10. Licensed under MIT.</p>
                </div>
            </div>
        </footer>`;
    }

    // ── Code-block headers ──────────────────────────────────
    // Authoring: <div class="code-block" data-title="Filename">
    //              <pre><code>...</code></pre>
    //            </div>
    // This function prepends the dots + filename header automatically.

    function enhanceCodeBlocks() {
        document.querySelectorAll('.code-block[data-title]').forEach(block => {
            const title = block.getAttribute('data-title');
            const header = document.createElement('div');
            header.className = 'code-header';
            header.innerHTML =
                '<div class="code-dots"><span></span><span></span><span></span></div>' +
                `<span class="code-filename">${title}</span>`;
            block.prepend(header);
        });
    }

    // ── Theme toggle ────────────────────────────────────────

    function initTheme() {
        const saved = localStorage.getItem('csharpdb-theme') || 'dark';
        document.documentElement.setAttribute('data-theme', saved);
        document.addEventListener('click', (e) => {
            if (e.target.closest('#themeToggle')) {
                const cur = document.documentElement.getAttribute('data-theme');
                const next = cur === 'dark' ? 'light' : 'dark';
                document.documentElement.setAttribute('data-theme', next);
                localStorage.setItem('csharpdb-theme', next);
            }
        });
    }

    // ── Mobile menu ─────────────────────────────────────────

    function initMobile() {
        document.addEventListener('click', (e) => {
            const btn = e.target.closest('#mobileToggle');
            if (btn) {
                document.getElementById('navLinks').classList.toggle('open');
                btn.classList.toggle('open');
            }
        });
    }

    // ── Navbar scroll shadow ────────────────────────────────

    function initNavScroll() {
        window.addEventListener('scroll', () => {
            const nb = document.getElementById('navbar');
            if (nb) nb.style.boxShadow = window.scrollY > 50 ? 'var(--shadow-md)' : 'none';
        }, { passive: true });
    }

    // ── Copy buttons ────────────────────────────────────────

    function initCopyButtons() {
        document.addEventListener('click', (e) => {
            const btn = e.target.closest('.copy-btn');
            if (btn) {
                const code = btn.parentElement.querySelector('code')?.textContent;
                if (code) {
                    navigator.clipboard.writeText(code).then(() => {
                        btn.style.color = 'var(--success)';
                        setTimeout(() => { btn.style.color = ''; }, 1500);
                    });
                }
            }
        });
    }

    // ── Bootstrap ───────────────────────────────────────────

    document.addEventListener('DOMContentLoaded', () => {
        renderMeta();
        renderNav();
        renderFooter();
        enhanceCodeBlocks();
        initTheme();
        initMobile();
        initNavScroll();
        initCopyButtons();
    });
})();
