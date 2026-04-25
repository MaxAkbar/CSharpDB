// ═══════════════════════════════════════════════════════════
// CSharpDB Website — Bundled JS (components + app)
// ═══════════════════════════════════════════════════════════

// ─── Shared Navigation & Footer Components ───
(function () {
    'use strict';

    const prefix = window.pagePathPrefix || '';

    const logoSvg = `<img src="${prefix}images/icon3.png" alt="CSharpDB" width="28" height="28">`;

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
                    ${navLink('docs/index.html', 'docs', 'Docs')}
                    ${navLink('downloads.html', 'downloads', 'Downloads')}
                    ${navLink('blog/index.html', 'blog', 'Blog')}
                    ${navLink('changelog.html', 'changelog', 'Changelog')}
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
                        <a href="${prefix}docs/getting-started.html">Getting Started</a>
                        <a href="${prefix}docs/index.html">Documentation</a>
                        <a href="${prefix}architecture.html">Architecture</a>
                        <a href="${prefix}docs/api-reference.html">API Reference</a>
                        <a href="${prefix}docs/performance.html">Performance Guide</a>
                        <a href="${prefix}roadmap.html">Roadmap</a>
                    </div>
                    <div class="footer-col">
                        <h4>Community</h4>
                        <a href="${prefix}blog/index.html">Blog</a>
                        <a href="${prefix}changelog.html">Changelog</a>
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

    function escapeHtml(value) {
        return value
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function inferCodeLanguage(code) {
        const blockTitle = code.closest('.code-block')?.dataset.title?.toLowerCase() || '';
        const className = code.className.toLowerCase();
        const text = code.textContent || '';

        if (blockTitle.includes('sql') || className.includes('language-sql')) return 'sql';
        if (blockTitle.includes('json') || className.includes('language-json') || /^\s*[{[]/.test(text)) return 'json';
        if (blockTitle.includes('powershell') || /\$env:|set-location|resolve-path/i.test(text)) return 'powershell';
        if (blockTitle.includes('bash') || className.includes('language-bash') || /(^|\n)\s*(dotnet|npm|node|cd|export)\b/.test(text)) return 'shell';
        if (blockTitle.includes('javascript') || className.includes('language-js') || /\b(import|const|let|function|await)\b[\s\S]*(from|require|console\.)/.test(text)) return 'javascript';
        if (blockTitle.includes('python') || className.includes('language-python') || /\b(def|from|import|with|None|True|False)\b/.test(text)) return 'python';
        if (blockTitle.includes('c#') || blockTitle.includes('program.cs') || className.includes('language-csharp')) return 'csharp';
        if (/\b(await|using|namespace|public|private|class|record|new|var|async|Task|Database|DbValue)\b/.test(text)) return 'csharp';

        return 'text';
    }

    function tokenClass(token, language) {
        const csharpKeywords = new Set([
            'abstract', 'as', 'async', 'await', 'base', 'bool', 'break', 'byte', 'case', 'catch',
            'class', 'const', 'continue', 'decimal', 'default', 'do', 'double', 'else', 'enum',
            'false', 'finally', 'float', 'for', 'foreach', 'if', 'in', 'int', 'interface',
            'internal', 'is', 'long', 'namespace', 'new', 'null', 'object', 'out', 'override',
            'private', 'protected', 'public', 'readonly', 'record', 'return', 'sealed', 'short',
            'static', 'string', 'struct', 'switch', 'this', 'throw', 'true', 'try', 'uint',
            'using', 'var', 'void', 'while'
        ]);
        const jsKeywords = new Set([
            'await', 'break', 'case', 'catch', 'class', 'const', 'continue', 'default', 'else',
            'export', 'false', 'finally', 'for', 'from', 'function', 'if', 'import', 'let', 'new',
            'null', 'return', 'throw', 'true', 'try', 'undefined', 'var', 'while'
        ]);
        const pythonKeywords = new Set([
            'as', 'async', 'await', 'break', 'class', 'def', 'elif', 'else', 'except', 'False',
            'finally', 'for', 'from', 'if', 'import', 'in', 'is', 'None', 'return', 'True', 'try',
            'with', 'while'
        ]);
        const sqlKeywords = new Set([
            'ADD', 'ALTER', 'AND', 'AS', 'ASC', 'BEGIN', 'BY', 'COMMIT', 'COUNT', 'CREATE',
            'DELETE', 'DESC', 'DISTINCT', 'DROP', 'EXEC', 'FROM', 'GROUP', 'INDEX', 'INSERT',
            'INTO', 'JOIN', 'KEY', 'LIMIT', 'NOT', 'NULL', 'ON', 'OR', 'ORDER', 'PRIMARY',
            'REAL', 'ROLLBACK', 'SELECT', 'SET', 'TABLE', 'TEXT', 'UPDATE', 'VALUES', 'VIEW',
            'WHERE'
        ]);
        const shellKeywords = new Set(['cd', 'dotnet', 'export', 'node', 'npm', 'python', 'set']);
        const tokenUpper = token.toUpperCase();

        if (/^(\/\/|\/\*)/.test(token)) return 'cm';
        if (token.startsWith('#') && (language === 'shell' || language === 'powershell' || language === 'python')) return 'cm';
        if (token.startsWith('--') && language === 'sql') return 'cm';
        if (/^(@?["']|"""|''')/.test(token)) return 'str';
        if (/^\d/.test(token)) return 'num';
        if (/^--[\w-]+$/.test(token) || /^\$[\w:]+$/.test(token)) return 'tp';

        if (language === 'sql' && sqlKeywords.has(tokenUpper)) return 'kw';
        if (language === 'shell' && shellKeywords.has(token)) return 'kw';
        if (language === 'powershell' && (/^[A-Z][A-Za-z]+-[A-Za-z]+$/.test(token) || /^\$[\w:]+$/.test(token))) return 'kw';
        if (language === 'javascript' && jsKeywords.has(token)) return 'kw';
        if (language === 'python' && pythonKeywords.has(token)) return 'kw';
        if (language === 'json' && /^(true|false|null)$/i.test(token)) return 'kw';
        if (language === 'csharp' && csharpKeywords.has(token)) return 'kw';
        if (language === 'csharp' && /^[A-Z][A-Za-z0-9_]*$/.test(token)) return 'tp';

        return '';
    }

    function highlightCodeText(source, language) {
        if (language === 'text') return escapeHtml(source);

        const commentPatterns = ['\\/\\*[\\s\\S]*?\\*\\/'];
        if (language === 'csharp' || language === 'javascript') commentPatterns.push('\\/\\/[^\\n]*');
        if (language === 'shell' || language === 'powershell' || language === 'python') commentPatterns.push('#[^\\n]*');
        if (language === 'sql') commentPatterns.push('--[^\\n]*');

        const tokenPattern = new RegExp(
            `(${commentPatterns.join('|')}|"""[\\s\\S]*?"""|'''[\\s\\S]*?'''|@?"(?:\\\\.|[^"\\\\])*"|'(?:\\\\.|[^'\\\\])*'|\\$[\\w:]+|--[\\w-]+|\\b\\d+(?:\\.\\d+)?\\b|\\b[A-Za-z_][A-Za-z0-9_]*\\b)`,
            'g'
        );
        let html = '';
        let index = 0;

        source.replace(tokenPattern, (token, _unused, offset) => {
            html += escapeHtml(source.slice(index, offset));
            const cls = tokenClass(token, language);
            const escaped = escapeHtml(token);
            html += cls ? `<span class="${cls}">${escaped}</span>` : escaped;
            index = offset + token.length;
            return token;
        });

        html += escapeHtml(source.slice(index));
        return html;
    }

    function initCodeHighlighting() {
        document.querySelectorAll('.doc-content pre code, .blog-post pre code').forEach(code => {
            if (code.dataset.highlighted === 'true') return;
            const language = inferCodeLanguage(code);
            code.innerHTML = highlightCodeText(code.textContent || '', language);
            code.dataset.highlighted = 'true';
        });
    }

    document.addEventListener('DOMContentLoaded', () => {
        renderNav();
        renderFooter();
        initTheme();
        initMobile();
        initNavScroll();
        initCopyButtons();
        initCodeHighlighting();
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

    // ─── API tab switcher ───
    document.addEventListener('DOMContentLoaded', () => {
        document.querySelectorAll('.api-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                const target = tab.dataset.tab;
                document.querySelectorAll('.api-tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.api-panel').forEach(p => p.classList.remove('active'));
                tab.classList.add('active');
                const panel = document.querySelector(`.api-panel[data-panel="${target}"]`);
                if (panel) panel.classList.add('active');
            });
        });
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
