// Theme persistence via localStorage
window.themeInterop = {
    get: () => localStorage.getItem('csharpdb-theme') || 'dark',
    set: (theme) => {
        localStorage.setItem('csharpdb-theme', theme);
        document.documentElement.setAttribute('data-theme', theme);
    }
};

window.fileInterop = {
    downloadText: (fileName, contentType, content) => {
        const blob = new Blob([content || ''], { type: contentType || 'text/plain;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = fileName || 'export.txt';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }
};

window.clipboardInterop = {
    writeText: (text) => navigator.clipboard.writeText(text || '')
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
        // Ctrl+K: command palette
        else if (e.ctrlKey && !e.shiftKey && e.key.toLowerCase() === 'k') {
            e.preventDefault();
            ref.invokeMethodAsync('OnKeyboardShortcut', 'OpenCommandPalette');
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
    _formEntryActive: false,
    _formEntryStartX: 0,
    _formEntryStartWidth: 0,
    _formEntryMin: 320,
    _formEntryMax: 720,
    _formEntryLayout: null,
    _queryEditorActive: false,
    _queryEditorStartY: 0,
    _queryEditorStartHeight: 0,
    _queryEditorMin: 120,
    _queryEditorMax: 560,
    _queryEditorLayout: null,
    _queryEditorDotNetRef: null,
    _callbacksActive: false,
    _callbacksStartX: 0,
    _callbacksStartY: 0,
    _callbacksStartSize: 0,
    _callbacksLayout: null,
    _callbacksTarget: null,
    _callbacksVariableName: '',
    _callbacksMin: 0,
    _callbacksMax: 0,
    _callbacksDirection: 1,
    _callbacksStorageKey: '',
    _callbacksAxis: 'y',
    _callbacksTargetName: '',

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
    },

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
    },

    initQueryEditorPane: (layout, fallbackHeight, minHeight, maxHeight) => {
        if (!layout) return fallbackHeight || 220;

        const min = minHeight || 120;
        const configuredMax = maxHeight || 560;
        const layoutMax = Math.max(min, layout.clientHeight - 140);
        const max = Math.max(min, Math.min(configuredMax, layoutMax));

        const storedHeight = parseInt(localStorage.getItem('csharpdb-query-editor-height') || '', 10);
        const baseHeight = Number.isFinite(storedHeight) ? storedHeight : (fallbackHeight || 220);
        const resolvedHeight = Math.max(min, Math.min(max, baseHeight));

        layout.style.setProperty('--query-editor-height', resolvedHeight + 'px');
        return resolvedHeight;
    },

    startQueryEditorResize: (e, layout, currentHeight, minHeight, maxHeight, dotNetRef) => {
        if (!layout) return;

        window.resizeInterop._queryEditorActive = true;
        window.resizeInterop._queryEditorStartY = e.clientY;
        window.resizeInterop._queryEditorStartHeight = currentHeight || 220;
        window.resizeInterop._queryEditorMin = minHeight || 120;
        window.resizeInterop._queryEditorMax = maxHeight || 560;
        window.resizeInterop._queryEditorLayout = layout;
        window.resizeInterop._queryEditorDotNetRef = dotNetRef || null;

        document.body.style.cursor = 'row-resize';
        document.body.style.userSelect = 'none';

        document.addEventListener('mousemove', window.resizeInterop._onQueryEditorMove);
        document.addEventListener('mouseup', window.resizeInterop._onQueryEditorUp);
    },

    _onQueryEditorMove: (e) => {
        if (!window.resizeInterop._queryEditorActive || !window.resizeInterop._queryEditorLayout) return;

        const dy = e.clientY - window.resizeInterop._queryEditorStartY;
        const min = window.resizeInterop._queryEditorMin;
        const configuredMax = window.resizeInterop._queryEditorMax;
        const layoutMax = Math.max(min, window.resizeInterop._queryEditorLayout.clientHeight - 140);
        const max = Math.max(min, Math.min(configuredMax, layoutMax));
        let nextHeight = window.resizeInterop._queryEditorStartHeight + dy;
        nextHeight = Math.max(min, Math.min(max, nextHeight));

        window.resizeInterop._queryEditorLayout.style.setProperty('--query-editor-height', nextHeight + 'px');
    },

    _onQueryEditorUp: () => {
        const layout = window.resizeInterop._queryEditorLayout;
        const dotNetRef = window.resizeInterop._queryEditorDotNetRef;

        if (layout) {
            const value = parseInt(getComputedStyle(layout).getPropertyValue('--query-editor-height') || '', 10);
            if (Number.isFinite(value)) {
                localStorage.setItem('csharpdb-query-editor-height', value + 'px');
                if (dotNetRef) {
                    dotNetRef.invokeMethodAsync('OnQueryEditorHeightChanged', value);
                }
            }
        }

        window.resizeInterop._queryEditorActive = false;
        window.resizeInterop._queryEditorLayout = null;
        window.resizeInterop._queryEditorDotNetRef = null;
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        document.removeEventListener('mousemove', window.resizeInterop._onQueryEditorMove);
        document.removeEventListener('mouseup', window.resizeInterop._onQueryEditorUp);
    },

    initCallbacksLayout: (layout) => {
        if (!layout) return;

        const storedSizes = [
            ['--callbacks-builtins-height', 'csharpdb-callbacks-builtins-height'],
            ['--callbacks-detail-width', 'csharpdb-callbacks-detail-width'],
            ['--callbacks-diagnostics-height', 'csharpdb-callbacks-diagnostics-height']
        ];

        for (const [variableName, storageKey] of storedSizes) {
            const storedValue = localStorage.getItem(storageKey);
            const size = parseInt(storedValue || '', 10);
            if (Number.isFinite(size) && size > 0) {
                layout.style.setProperty(variableName, size + 'px');
            }
        }
    },

    startCallbacksResize: (e, layout, targetElement, targetName, variableName, minSize, maxSize, direction, storageKey) => {
        if (!layout || !variableName) return;

        if (window.resizeInterop._callbacksActive) {
            window.resizeInterop._onCallbacksResizeUp();
        }

        const target = window.resizeInterop._resolveCallbacksResizeTarget(layout, targetElement, targetName);
        const axis = targetName === 'detail' ? 'x' : 'y';
        const startSize = window.resizeInterop._getCallbacksResizeSize(layout, target, targetName, variableName, axis);

        window.resizeInterop._callbacksActive = true;
        window.resizeInterop._callbacksStartX = e?.clientX || 0;
        window.resizeInterop._callbacksStartY = e?.clientY || 0;
        window.resizeInterop._callbacksStartSize = startSize;
        window.resizeInterop._callbacksLayout = layout;
        window.resizeInterop._callbacksTarget = target;
        window.resizeInterop._callbacksTargetName = targetName || '';
        window.resizeInterop._callbacksVariableName = variableName;
        window.resizeInterop._callbacksMin = minSize || 0;
        window.resizeInterop._callbacksMax = maxSize || 1000;
        window.resizeInterop._callbacksDirection = direction || 1;
        window.resizeInterop._callbacksStorageKey = storageKey || '';
        window.resizeInterop._callbacksAxis = axis;

        document.body.style.cursor = axis === 'x' ? 'col-resize' : 'row-resize';
        document.body.style.userSelect = 'none';

        document.addEventListener('mousemove', window.resizeInterop._onCallbacksResizeMove);
        document.addEventListener('mouseup', window.resizeInterop._onCallbacksResizeUp);
    },

    _resolveCallbacksResizeTarget: (layout, targetElement, targetName) => {
        if (targetElement && typeof targetElement.getBoundingClientRect === 'function') {
            return targetElement;
        }

        if (!layout || typeof layout.querySelector !== 'function') {
            return null;
        }

        if (targetName === 'builtins') {
            return layout.querySelector('.callbacks-builtins-panel');
        }

        if (targetName === 'diagnostics') {
            return layout.querySelector('.callbacks-diagnostics-panel');
        }

        if (targetName === 'detail') {
            return layout.querySelector('.callbacks-detail-panel');
        }

        return null;
    },

    _getCallbacksResizeSize: (layout, target, targetName, variableName, axis) => {
        const targetSize = axis === 'x'
            ? target?.offsetWidth
            : target?.offsetHeight;

        if (Number.isFinite(targetSize) && targetSize > 0) {
            return targetSize;
        }

        const cssSize = parseInt(getComputedStyle(layout).getPropertyValue(variableName) || '', 10);
        if (Number.isFinite(cssSize) && cssSize > 0) {
            return cssSize;
        }

        if (targetName === 'detail') return 420;
        if (targetName === 'diagnostics') return 240;
        return 240;
    },

    _getCallbacksResizeMax: () => {
        const layout = window.resizeInterop._callbacksLayout;
        const min = window.resizeInterop._callbacksMin;
        const configuredMax = window.resizeInterop._callbacksMax;
        const targetName = window.resizeInterop._callbacksTargetName;
        const axis = window.resizeInterop._callbacksAxis;

        if (!layout) {
            return Math.max(min, configuredMax);
        }

        if (axis === 'x') {
            const content = layout.querySelector?.('.callbacks-layout');
            const contentWidth = content?.clientWidth || layout.clientWidth || window.innerWidth;
            const layoutMax = Math.max(min, contentWidth - 368);
            return Math.max(min, Math.min(configuredMax, layoutMax));
        }

        const toolbar = layout.querySelector?.('.data-toolbar');
        const builtins = layout.querySelector?.('.callbacks-builtins-panel');
        const diagnostics = layout.querySelector?.('.callbacks-diagnostics-panel');
        const toolbarHeight = toolbar?.offsetHeight || 0;
        const builtinsHeight = builtins?.offsetHeight || 0;
        const diagnosticsHeight = diagnostics?.offsetHeight || 0;
        const reservedMainHeight = 180;
        const splitterHeight = 16;
        const otherResizableHeight = targetName === 'builtins' ? diagnosticsHeight : builtinsHeight;
        const layoutMax = Math.max(min, (layout.clientHeight || window.innerHeight) - toolbarHeight - splitterHeight - otherResizableHeight - reservedMainHeight);

        return Math.max(min, Math.min(configuredMax, layoutMax));
    },

    _onCallbacksResizeMove: (e) => {
        if (!window.resizeInterop._callbacksActive || !window.resizeInterop._callbacksLayout) return;

        const axis = window.resizeInterop._callbacksAxis;
        const delta = axis === 'x'
            ? e.clientX - window.resizeInterop._callbacksStartX
            : e.clientY - window.resizeInterop._callbacksStartY;
        const min = window.resizeInterop._callbacksMin;
        const max = window.resizeInterop._getCallbacksResizeMax();
        let nextSize = window.resizeInterop._callbacksStartSize + (delta * window.resizeInterop._callbacksDirection);

        nextSize = Math.max(min, Math.min(max, nextSize));
        window.resizeInterop._callbacksLayout.style.setProperty(window.resizeInterop._callbacksVariableName, Math.round(nextSize) + 'px');
    },

    _onCallbacksResizeUp: () => {
        const layout = window.resizeInterop._callbacksLayout;
        const variableName = window.resizeInterop._callbacksVariableName;
        const storageKey = window.resizeInterop._callbacksStorageKey;

        if (layout && variableName && storageKey) {
            const value = parseInt(getComputedStyle(layout).getPropertyValue(variableName) || '', 10);
            if (Number.isFinite(value)) {
                localStorage.setItem(storageKey, value + 'px');
            }
        }

        window.resizeInterop._callbacksActive = false;
        window.resizeInterop._callbacksLayout = null;
        window.resizeInterop._callbacksTarget = null;
        window.resizeInterop._callbacksVariableName = '';
        window.resizeInterop._callbacksStorageKey = '';
        window.resizeInterop._callbacksTargetName = '';
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
        document.removeEventListener('mousemove', window.resizeInterop._onCallbacksResizeMove);
        document.removeEventListener('mouseup', window.resizeInterop._onCallbacksResizeUp);
    }
};

// Query Designer — node drag and canvas splitter
window.designerInterop = {
    _dotNetRef: null,
    _initialized: false,

    // Called once on first render. Subsequent calls just update the dotNetRef.
    // Uses document-level listeners so there are no timing or element-reference issues.
    initDrag: (dotNetRef) => {
        window.designerInterop._dotNetRef = dotNetRef;
        if (window.designerInterop._initialized) return;
        window.designerInterop._initialized = true;

        let dragging = false, node = null, tableName = '', startX = 0, startY = 0, origLeft = 0, origTop = 0;

        document.addEventListener('mousedown', (e) => {
            const header = e.target.closest('.designer-table-node-header');
            if (!header) return;
            const n = header.closest('.designer-table-node');
            if (!n) return;
            e.preventDefault();
            dragging = true;
            node = n;
            tableName = n.dataset.table;
            startX = e.clientX;
            startY = e.clientY;
            origLeft = parseFloat(n.style.left) || 0;
            origTop  = parseFloat(n.style.top)  || 0;
        });

        document.addEventListener('mousemove', (e) => {
            if (!dragging || !node) return;
            node.style.left = Math.max(0, origLeft + e.clientX - startX) + 'px';
            node.style.top  = Math.max(0, origTop  + e.clientY - startY) + 'px';
        });

        document.addEventListener('mouseup', () => {
            if (!dragging || !node) return;
            dragging = false;
            const left = parseFloat(node.style.left) || 0;
            const top  = parseFloat(node.style.top)  || 0;
            const ref  = window.designerInterop._dotNetRef;
            const name = tableName;
            node = null;
            if (ref) ref.invokeMethodAsync('OnTableMoved', name, left, top);
        });
    },

    dispose: () => {
        window.designerInterop._dotNetRef = null;
    },

    // Vertical splitter drag — resizes the canvas height.
    // Calls dotNetRef.invokeMethodAsync('OnSplitterMoved', newHeight).
    startSplitterDrag: (e, canvasElement, dotNetRef) => {
        const canvas = canvasElement;
        if (!canvas) return;

        const startY  = e.clientY;
        const startH  = canvas.offsetHeight;

        const onMove = (ev) => {
            const dy = ev.clientY - startY;
            const newH = Math.max(100, Math.min(800, startH + dy));
            canvas.style.height = newH + 'px';
        };

        const onUp = () => {
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
            const finalH = parseFloat(canvas.style.height) || startH;
            dotNetRef.invokeMethodAsync('OnSplitterMoved', Math.round(finalH));
        };

        document.body.style.cursor = 'row-resize';
        document.body.style.userSelect = 'none';
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
    },

    startPreviewSplitterDrag: (e, previewElement, dotNetRef, currentHeight) => {
        const preview = previewElement;
        if (!preview) return;

        const startY = e.clientY;
        const startH = currentHeight || preview.offsetHeight || 132;

        const onMove = (ev) => {
            const dy = ev.clientY - startY;
            const newH = Math.max(96, Math.min(320, startH + dy));
            preview.style.height = newH + 'px';
        };

        const onUp = () => {
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
            const finalH = parseFloat(preview.style.height) || startH;
            dotNetRef.invokeMethodAsync('OnSqlPreviewHeightMoved', Math.round(finalH));
        };

        document.body.style.cursor = 'row-resize';
        document.body.style.userSelect = 'none';
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
    }
};

// SQL editor scroll sync
window.editorInterop = {
    _editors: new Map(),

    initSqlEditor: (editorId, dotNetRef) => {
        const textarea = document.getElementById(editorId);
        if (!textarea) return;

        window.editorInterop.disposeSqlEditor(editorId);

        const keydown = (e) => {
            if (e.ctrlKey && e.key === ' ') {
                e.preventDefault();
                dotNetRef.invokeMethodAsync('OnSqlEditorShortcut', 'Complete');
                return;
            }

            const completionOpen = !!textarea
                .closest('.sql-editor-area')
                ?.querySelector('.sql-completion-popup');

            if (!completionOpen) return;

            if (e.key === 'ArrowDown' || e.key === 'ArrowUp' || e.key === 'Enter' || e.key === 'Tab' || e.key === 'Escape') {
                e.preventDefault();
                dotNetRef.invokeMethodAsync('OnSqlEditorCompletionKey', e.key);
            }
        };

        textarea.addEventListener('keydown', keydown);
        window.editorInterop._editors.set(editorId, { keydown });
    },

    disposeSqlEditor: (editorId) => {
        const registration = window.editorInterop._editors.get(editorId);
        if (!registration) return;

        const textarea = document.getElementById(editorId);
        if (textarea) {
            textarea.removeEventListener('keydown', registration.keydown);
        }

        window.editorInterop._editors.delete(editorId);
    },

    getEditorState: (editorId) => {
        const textarea = document.getElementById(editorId);
        if (!textarea) return { value: '', selectionStart: 0, selectionEnd: 0 };

        return {
            value: textarea.value,
            selectionStart: textarea.selectionStart || 0,
            selectionEnd: textarea.selectionEnd || 0
        };
    },

    replaceEditorText: (editorId, start, end, insertText, caretPosition) => {
        const textarea = document.getElementById(editorId);
        if (!textarea) return { value: '', selectionStart: 0, selectionEnd: 0 };

        const value = textarea.value || '';
        const safeStart = Math.max(0, Math.min(start || 0, value.length));
        const safeEnd = Math.max(safeStart, Math.min(end || safeStart, value.length));
        const nextValue = value.slice(0, safeStart) + (insertText || '') + value.slice(safeEnd);
        const nextCaret = Math.max(0, Math.min(caretPosition ?? (safeStart + (insertText || '').length), nextValue.length));

        textarea.value = nextValue;
        textarea.focus();
        textarea.setSelectionRange(nextCaret, nextCaret);
        textarea.dispatchEvent(new Event('input', { bubbles: true }));

        return {
            value: nextValue,
            selectionStart: nextCaret,
            selectionEnd: nextCaret
        };
    },

    getCaretCoordinates: (editorId) => {
        const textarea = document.getElementById(editorId);
        if (!textarea) return { left: 12, top: 30 };

        const style = window.getComputedStyle(textarea);
        const mirror = document.createElement('div');
        mirror.style.position = 'absolute';
        mirror.style.visibility = 'hidden';
        mirror.style.whiteSpace = 'pre-wrap';
        mirror.style.wordWrap = 'break-word';
        mirror.style.overflowWrap = 'break-word';
        mirror.style.boxSizing = style.boxSizing;
        mirror.style.width = textarea.clientWidth + 'px';
        mirror.style.font = style.font;
        mirror.style.fontFamily = style.fontFamily;
        mirror.style.fontSize = style.fontSize;
        mirror.style.fontWeight = style.fontWeight;
        mirror.style.lineHeight = style.lineHeight;
        mirror.style.letterSpacing = style.letterSpacing;
        mirror.style.padding = style.padding;
        mirror.style.border = style.border;
        mirror.style.left = '-9999px';
        mirror.style.top = '0';

        const before = (textarea.value || '').slice(0, textarea.selectionStart || 0);
        mirror.appendChild(document.createTextNode(before));
        const marker = document.createElement('span');
        marker.textContent = '\u200b';
        mirror.appendChild(marker);
        document.body.appendChild(mirror);

        const lineHeight = parseFloat(style.lineHeight) || 19.2;
        const area = textarea.closest('.sql-editor-area');
        const areaWidth = area?.clientWidth || textarea.clientWidth;
        const left = Math.min(Math.max(marker.offsetLeft - textarea.scrollLeft, 8), Math.max(8, areaWidth - 280));
        const top = marker.offsetTop - textarea.scrollTop + lineHeight + 4;

        document.body.removeChild(mirror);

        return { left, top };
    },

    syncScroll: (editorId) => {
        const textarea = document.getElementById(editorId);
        if (!textarea) return;
        const overlay = textarea.previousElementSibling;
        const lineNums = textarea.closest('.sql-editor-wrapper')?.querySelector('.sql-line-numbers');
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
