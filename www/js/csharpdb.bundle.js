// ═══════════════════════════════════════════════════════════
// CSharpDB Website — Bundled JS (components + app)
// ═══════════════════════════════════════════════════════════

// ─── Shared Navigation & Footer Components ───
(function () {
    'use strict';

    const prefix = window.pagePathPrefix || '';

    const logoSvg = `<svg width="28" height="28" viewBox="0 0 28 28" fill="none">
        <rect x="2" y="2" width="24" height="24" rx="6" stroke="currentColor" stroke-width="2"/>
        <ellipse cx="14" cy="10" rx="8" ry="3" stroke="currentColor" stroke-width="1.5"/>
        <path d="M6 10v4c0 1.66 3.58 3 8 3s8-1.34 8-3v-4" stroke="currentColor" stroke-width="1.5"/>
        <path d="M6 14v4c0 1.66 3.58 3 8 3s8-1.34 8-3v-4" stroke="currentColor" stroke-width="1.5"/>
    </svg>`;

    const sunSvg = `<svg class="icon-sun" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="5"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/></svg>`;
    const moonSvg = `<svg class="icon-moon" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>`;

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

    function initMobile() {
        document.addEventListener('click', (e) => {
            const btn = e.target.closest('#mobileToggle');
            if (btn) {
                document.getElementById('navLinks').classList.toggle('open');
                btn.classList.toggle('open');
            }
        });
    }

    function initNavScroll() {
        window.addEventListener('scroll', () => {
            const nb = document.getElementById('navbar');
            if (nb) nb.style.boxShadow = window.scrollY > 50 ? 'var(--shadow-md)' : 'none';
        }, { passive: true });
    }

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

    document.addEventListener('DOMContentLoaded', () => {
        renderNav();
        renderFooter();
        initTheme();
        initMobile();
        initNavScroll();
        initCopyButtons();
    });
})();

// ─── Page-specific Interactions ───
(function () {
    'use strict';

    // ─── Doc Section Router (for pages with sidebar tabs) ───
    function navigateToDoc(docId) {
        document.querySelectorAll('.doc-section').forEach(s => s.classList.remove('active'));
        const target = document.getElementById('doc-' + docId);
        if (target) target.classList.add('active');

        document.querySelectorAll('.doc-sidebar .sidebar-link').forEach(link => {
            link.classList.toggle('active', link.dataset.doc === docId);
        });

        const content = document.querySelector('.doc-content');
        if (content) content.scrollTop = 0;
        window.scrollTo({ top: 0, behavior: 'instant' });
    }

    // ─── API Section Router ───
    function navigateToApi(apiId) {
        document.querySelectorAll('.api-section').forEach(s => s.classList.remove('active'));
        const target = document.getElementById('api-' + apiId);
        if (target) target.classList.add('active');

        document.querySelectorAll('.doc-sidebar .sidebar-link').forEach(link => {
            link.classList.toggle('active', link.dataset.api === apiId);
        });
    }

    // ─── Click Handlers ───
    document.addEventListener('click', (e) => {
        // Doc sidebar links
        const docLink = e.target.closest('[data-doc]');
        if (docLink) {
            e.preventDefault();
            navigateToDoc(docLink.dataset.doc);
            return;
        }

        // API sidebar links
        const apiLink = e.target.closest('[data-api]');
        if (apiLink) {
            e.preventDefault();
            navigateToApi(apiLink.dataset.api);
            return;
        }
    });

    // ─── Animate Benchmark Bars on Scroll ───
    const benchObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.querySelectorAll('.bench-bar').forEach((bar, i) => {
                    const w = bar.style.width;
                    bar.style.width = '0%';
                    setTimeout(() => { bar.style.width = w; }, i * 150 + 100);
                });
                benchObserver.unobserve(entry.target);
            }
        });
    }, { threshold: 0.3 });

    document.addEventListener('DOMContentLoaded', () => {
        document.querySelectorAll('.bench-chart-container').forEach(c => benchObserver.observe(c));
    });

    // ─── Animate Features on Scroll ───
    const featureObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.querySelectorAll('.feature-card, .hub-card').forEach((card, i) => {
                    card.style.opacity = '0';
                    card.style.transform = 'translateY(20px)';
                    setTimeout(() => {
                        card.style.transition = 'opacity 0.5s ease, transform 0.5s ease';
                        card.style.opacity = '1';
                        card.style.transform = 'translateY(0)';
                    }, i * 80);
                });
                featureObserver.unobserve(entry.target);
            }
        });
    }, { threshold: 0.15 });

    document.addEventListener('DOMContentLoaded', () => {
        document.querySelectorAll('.features-grid, .hub-grid').forEach(g => featureObserver.observe(g));
    });

    // ─── Animate Stats on Scroll ───
    const statsObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.querySelectorAll('.stat').forEach((stat, i) => {
                    stat.style.opacity = '0';
                    stat.style.transform = 'scale(0.9)';
                    setTimeout(() => {
                        stat.style.transition = 'opacity 0.4s ease, transform 0.4s ease';
                        stat.style.opacity = '1';
                        stat.style.transform = 'scale(1)';
                    }, i * 100);
                });
                statsObserver.unobserve(entry.target);
            }
        });
    }, { threshold: 0.3 });

    document.addEventListener('DOMContentLoaded', () => {
        document.querySelectorAll('.stats-grid').forEach(g => statsObserver.observe(g));
    });

    // ─── Handle hash-based doc section navigation ───
    document.addEventListener('DOMContentLoaded', () => {
        const hash = window.location.hash.replace('#', '');
        if (hash) {
            const docEl = document.getElementById('doc-' + hash);
            const apiEl = document.getElementById('api-' + hash);
            if (docEl) navigateToDoc(hash);
            if (apiEl) navigateToApi(hash);
        }
    });

})();
