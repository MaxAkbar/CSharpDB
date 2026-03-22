// ═══════════════════════════════════════════════════════════
// CSharpDB Website — Page-specific Interactions
// ═══════════════════════════════════════════════════════════

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
