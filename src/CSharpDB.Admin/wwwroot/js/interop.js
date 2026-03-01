// Theme persistence via localStorage
window.themeInterop = {
    get: () => localStorage.getItem('csharpdb-theme') || 'dark',
    set: (theme) => {
        localStorage.setItem('csharpdb-theme', theme);
        document.documentElement.setAttribute('data-theme', theme);
    }
};

// Keyboard shortcut listener - invokes .NET methods
window.keyboardInterop = {
    _dotNetRef: null,

    init: (dotNetRef) => {
        window.keyboardInterop._dotNetRef = dotNetRef;
        document.addEventListener('keydown', window.keyboardInterop._handler);
    },

    dispose: () => {
        document.removeEventListener('keydown', window.keyboardInterop._handler);
        window.keyboardInterop._dotNetRef = null;
    },

    _handler: (e) => {
        const ref = window.keyboardInterop._dotNetRef;
        if (!ref) return;

        // Ctrl+Enter: Run query
        if (e.ctrlKey && e.key === 'Enter') {
            e.preventDefault();
            ref.invokeMethodAsync('OnKeyboardShortcut', 'RunQuery');
        }
        // Ctrl+N: New query tab
        else if (e.ctrlKey && !e.shiftKey && e.key === 'n') {
            e.preventDefault();
            ref.invokeMethodAsync('OnKeyboardShortcut', 'NewQuery');
        }
        // Ctrl+B: Toggle sidebar
        else if (e.ctrlKey && e.key === 'b') {
            e.preventDefault();
            ref.invokeMethodAsync('OnKeyboardShortcut', 'ToggleSidebar');
        }
        // Ctrl+Shift+L: Toggle theme
        else if (e.ctrlKey && e.shiftKey && e.key === 'L') {
            e.preventDefault();
            ref.invokeMethodAsync('OnKeyboardShortcut', 'ToggleTheme');
        }
        // Ctrl+W: Close tab
        else if (e.ctrlKey && e.key === 'w') {
            e.preventDefault();
            ref.invokeMethodAsync('OnKeyboardShortcut', 'CloseTab');
        }
    }
};

// Sidebar resize via drag
window.resizeInterop = {
    _active: false,
    _startX: 0,
    _startWidth: 0,
    _dotNetRef: null,
    _min: 200,
    _max: 500,

    initSidebar: (dotNetRef, minWidth, maxWidth) => {
        window.resizeInterop._dotNetRef = dotNetRef;
        window.resizeInterop._min = minWidth || 200;
        window.resizeInterop._max = maxWidth || 500;
    },

    startResize: (e) => {
        window.resizeInterop._active = true;
        window.resizeInterop._startX = e.clientX;
        const sidebar = document.querySelector('.sidebar');
        if (sidebar) {
            window.resizeInterop._startWidth = sidebar.offsetWidth;
        }
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';

        document.addEventListener('mousemove', window.resizeInterop._onMove);
        document.addEventListener('mouseup', window.resizeInterop._onUp);
    },

    _onMove: (e) => {
        if (!window.resizeInterop._active) return;
        const dx = e.clientX - window.resizeInterop._startX;
        let newWidth = window.resizeInterop._startWidth + dx;
        newWidth = Math.max(window.resizeInterop._min, Math.min(window.resizeInterop._max, newWidth));
        document.documentElement.style.setProperty('--sidebar-width', newWidth + 'px');
    },

    _onUp: () => {
        window.resizeInterop._active = false;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        document.removeEventListener('mousemove', window.resizeInterop._onMove);
        document.removeEventListener('mouseup', window.resizeInterop._onUp);
    }
};

// SQL editor scroll sync
window.editorInterop = {
    syncScroll: (editorId) => {
        const textarea = document.getElementById(editorId);
        if (!textarea) return;
        const overlay = textarea.previousElementSibling;
        const lineNums = textarea.parentElement?.querySelector('.sql-line-numbers');
        if (overlay) {
            overlay.scrollTop = textarea.scrollTop;
            overlay.scrollLeft = textarea.scrollLeft;
        }
        if (lineNums) {
            lineNums.scrollTop = textarea.scrollTop;
        }
    },

    focus: (elementId) => {
        const el = document.getElementById(elementId);
        if (el) el.focus();
    }
};
