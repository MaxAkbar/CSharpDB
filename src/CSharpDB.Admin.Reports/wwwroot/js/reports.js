window.reportInterop = {
    printPages: (pagesElement, title) => {
        if (!pagesElement) {
            return;
        }

        const iframe = document.createElement('iframe');
        iframe.setAttribute('aria-hidden', 'true');
        iframe.style.position = 'fixed';
        iframe.style.width = '0';
        iframe.style.height = '0';
        iframe.style.border = '0';
        iframe.style.right = '0';
        iframe.style.bottom = '0';

        document.body.appendChild(iframe);

        const frameWindow = iframe.contentWindow;
        const frameDocument = frameWindow?.document;
        if (!frameWindow || !frameDocument) {
            iframe.remove();
            return;
        }

        const styleMarkup = Array.from(document.head.querySelectorAll('link[rel="stylesheet"], style'))
            .map(node => node.outerHTML)
            .join('\n');

        const safeTitle = typeof title === 'string'
            ? title.replace(/[&<>"]/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[ch]))
            : 'Report Preview';
        const theme = document.documentElement.getAttribute('data-theme') || 'dark';

        frameDocument.open();
        frameDocument.write(`<!DOCTYPE html>
<html data-theme="${theme}">
<head>
    <meta charset="utf-8">
    <title>${safeTitle}</title>
    ${styleMarkup}
    <style>
        html, body {
            margin: 0;
            padding: 0;
            background: #fff;
        }

        body {
            -webkit-print-color-adjust: exact;
            print-color-adjust: exact;
            color: #000;
        }

        .report-print-root {
            --text-primary: #000 !important;
            --text-secondary: #000 !important;
            --text-muted: #000 !important;
            --report-accent-text: #000 !important;
            --report-line-color: #000 !important;
            --report-page-glow: transparent !important;
            --report-page-bg: #fff !important;
            --report-paper: #fff !important;
            --report-control-bg: transparent !important;
            --report-control-border: transparent !important;
            --report-border: #000 !important;
            --report-shadow: none !important;
            min-height: auto !important;
            padding: 0 !important;
            margin: 0 !important;
            color: #000 !important;
        }

        .report-pages {
            padding: 0 !important;
        }

        .report-page {
            width: 100% !important;
        }

        .report-page-surface {
            width: 100% !important;
            max-width: none !important;
            overflow: visible !important;
            background: #fff !important;
        }

        .report-print-root,
        .report-print-root * {
            color: #000 !important;
            text-shadow: none !important;
            opacity: 1 !important;
        }

        .report-render-control.line {
            background: #000 !important;
        }

        .report-render-control.box {
            border-color: #000 !important;
        }
    </style>
</head>
<body></body>
</html>`);
        frameDocument.close();

        const wrapper = frameDocument.createElement('div');
        wrapper.className = 'report-preview-root report-print-root';
        wrapper.appendChild(pagesElement.cloneNode(true));
        frameDocument.body.appendChild(wrapper);

        const cleanup = () => {
            frameWindow.onafterprint = null;
            setTimeout(() => iframe.remove(), 250);
        };

        frameWindow.onafterprint = cleanup;

        setTimeout(() => {
            frameWindow.focus();
            frameWindow.print();
            setTimeout(cleanup, 1500);
        }, 300);
    }
};
