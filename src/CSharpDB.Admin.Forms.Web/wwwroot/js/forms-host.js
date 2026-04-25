window.formsTheme = {
    get: () => localStorage.getItem('csharpdb-theme') || 'dark',

    set: (theme) => {
        const nextTheme = theme === 'light' ? 'light' : 'dark';
        localStorage.setItem('csharpdb-theme', nextTheme);
        document.documentElement.setAttribute('data-theme', nextTheme);
        window.formsTheme.updateControls();
    },

    toggle: () => {
        window.formsTheme.set(window.formsTheme.get() === 'dark' ? 'light' : 'dark');
    },

    updateControls: () => {
        const theme = window.formsTheme.get();
        const label = theme === 'dark' ? 'Dark' : 'Light';
        const icon = theme === 'dark' ? 'D' : 'L';

        document.querySelectorAll('[data-theme-label]').forEach(element => {
            element.textContent = label;
        });

        document.querySelectorAll('[data-theme-icon]').forEach(element => {
            element.textContent = icon;
        });
    },

    init: () => {
        window.formsTheme.set(window.formsTheme.get());
    }
};

document.addEventListener('DOMContentLoaded', () => {
    window.formsTheme.init();
});

window.resizeInterop = {
    _formEntryActive: false,
    _formEntryStartX: 0,
    _formEntryStartWidth: 0,
    _formEntryMin: 320,
    _formEntryMax: 720,
    _formEntryLayout: null,

    initFormEntryPane: (layout) => {
        if (!layout) return;

        const savedWidth = localStorage.getItem('csharpdb-form-entry-record-pane-width');
        if (savedWidth) {
            layout.style.setProperty('--de-record-pane-width', savedWidth);
        }
    },

    startFormEntryResize: (e, layout, pane, minWidth, maxWidth) => {
        if (!layout || !pane) return;

        window.resizeInterop._formEntryActive = true;
        window.resizeInterop._formEntryStartX = e.clientX;
        window.resizeInterop._formEntryStartWidth = pane.offsetWidth;
        window.resizeInterop._formEntryMin = minWidth || 320;
        window.resizeInterop._formEntryMax = maxWidth || 720;
        window.resizeInterop._formEntryLayout = layout;

        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';

        document.addEventListener('mousemove', window.resizeInterop._onFormEntryMove);
        document.addEventListener('mouseup', window.resizeInterop._onFormEntryUp);
    },

    _onFormEntryMove: (e) => {
        if (!window.resizeInterop._formEntryActive || !window.resizeInterop._formEntryLayout) return;

        const dx = e.clientX - window.resizeInterop._formEntryStartX;
        const minWidth = window.resizeInterop._formEntryMin;
        const absoluteMax = Math.max(minWidth, Math.min(window.resizeInterop._formEntryMax, window.innerWidth - 360));
        let newWidth = window.resizeInterop._formEntryStartWidth - dx;
        newWidth = Math.max(minWidth, Math.min(absoluteMax, newWidth));

        window.resizeInterop._formEntryLayout.style.setProperty('--de-record-pane-width', newWidth + 'px');
    },

    _onFormEntryUp: () => {
        if (window.resizeInterop._formEntryLayout) {
            const width = getComputedStyle(window.resizeInterop._formEntryLayout)
                .getPropertyValue('--de-record-pane-width')
                .trim();
            if (width) {
                localStorage.setItem('csharpdb-form-entry-record-pane-width', width);
            }
        }

        window.resizeInterop._formEntryActive = false;
        window.resizeInterop._formEntryLayout = null;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        document.removeEventListener('mousemove', window.resizeInterop._onFormEntryMove);
        document.removeEventListener('mouseup', window.resizeInterop._onFormEntryUp);
    }
};
