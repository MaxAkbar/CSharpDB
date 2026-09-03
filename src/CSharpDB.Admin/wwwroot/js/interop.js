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

window.desktopShellInterop = (() => {
    const requestType = 'desktop-shell-dialog-request';
    const resultType = 'desktop-shell-dialog-result';
    const bridgeClassName = 'desktop-shell-bridge';
    const pending = new Map();
    let fallbackRequestId = 0;
    let subscribedBridge = null;

    const getBridge = () => {
        const bridge = window.chrome?.webview;
        return bridge &&
            typeof bridge.postMessage === 'function' &&
            typeof bridge.addEventListener === 'function'
            ? bridge
            : null;
    };

    const syncBridgeAvailability = () => {
        const available = getBridge() !== null;
        document.documentElement.classList.toggle(bridgeClassName, available);
        return available;
    };

    syncBridgeAvailability();

    const onMessage = (event) => {
        const message = event?.data;
        if (!message ||
            typeof message !== 'object' ||
            message.type !== resultType ||
            typeof message.requestId !== 'string') {
            return;
        }

        const request = pending.get(message.requestId);
        if (!request) return;

        pending.delete(message.requestId);
        if (typeof message.error === 'string' && message.error.length > 0) {
            request.reject(new Error(message.error));
            return;
        }

        request.resolve(message);
    };

    const ensureBridge = () => {
        const bridge = getBridge();
        if (!bridge) return null;

        if (subscribedBridge !== bridge) {
            if (subscribedBridge && typeof subscribedBridge.removeEventListener === 'function') {
                subscribedBridge.removeEventListener('message', onMessage);
            }
            bridge.addEventListener('message', onMessage);
            subscribedBridge = bridge;
        }

        return bridge;
    };

    const createRequestId = () => {
        if (typeof window.crypto?.randomUUID === 'function') {
            return window.crypto.randomUUID();
        }

        fallbackRequestId += 1;
        return `${Date.now()}-${fallbackRequestId}`;
    };

    const requestDialog = (dialog, options) => new Promise((resolve, reject) => {
        const bridge = ensureBridge();
        if (!bridge) {
            reject(new Error('The native desktop dialog bridge is not available.'));
            return;
        }

        const requestId = createRequestId();
        pending.set(requestId, { resolve, reject });

        try {
            bridge.postMessage({
                type: requestType,
                requestId,
                dialog,
                options: options && typeof options === 'object' && !Array.isArray(options)
                    ? options
                    : {}
            });
        } catch (error) {
            pending.delete(requestId);
            reject(error instanceof Error ? error : new Error(String(error)));
        }
    });

    const selectPath = async (dialog, options) => {
        try {
            const result = await requestDialog(dialog, options);
            return result.canceled === true ? null : (typeof result.path === 'string' ? result.path : null);
        } catch (error) {
            // Native picker failures should not escape a Blazor event callback and
            // tear down the interactive circuit. Keep the editable path field as
            // the fallback and leave a diagnostic in the WebView console.
            console.error('Native path picker failed.', error);
            return null;
        }
    };

    const pickPath = (options) => {
        if (!syncBridgeAvailability()) return Promise.resolve(null);

        const kind = options?.kind;
        const dialogs = {
            openFile: 'open-file',
            saveFile: 'save-file',
            folder: 'open-folder'
        };
        const dialog = Object.prototype.hasOwnProperty.call(dialogs, kind)
            ? dialogs[kind]
            : null;
        if (!dialog) {
            return Promise.reject(new Error(`Unsupported native path picker kind '${kind ?? ''}'.`));
        }

        return selectPath(dialog, options);
    };

    return {
        isAvailable: syncBridgeAvailability,
        openDatabase: () => {
            if (!syncBridgeAvailability()) return false;

            // Opening a database reloads the WebView after the host switches its
            // connection. Report bridge acceptance synchronously so that reload is
            // not part of the JavaScript interop result.
            requestDialog('open-database', {}).catch((error) => {
                console.error('Native database dialog failed.', error);
            });
            return true;
        },
        pickPath
    };
})();

window.contextMenuInterop = {
    position: (menu, requestedX, requestedY) => {
        if (!menu) return;

        const margin = 8;
        const viewportWidth = window.innerWidth || document.documentElement.clientWidth || 0;
        const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;

        menu.style.maxHeight = '';
        menu.style.left = `${Math.max(margin, requestedX)}px`;
        menu.style.top = `${Math.max(margin, requestedY)}px`;

        const rect = menu.getBoundingClientRect();
        const maxHeight = Math.max(120, viewportHeight - (margin * 2));
        let nextX = requestedX;
        let nextY = requestedY;

        if (nextX + rect.width + margin > viewportWidth) {
            nextX = viewportWidth - rect.width - margin;
        }

        if (rect.height > maxHeight) {
            nextY = margin;
            menu.style.maxHeight = `${maxHeight}px`;
        } else if (nextY + rect.height + margin > viewportHeight) {
            nextY = viewportHeight - rect.height - margin;
        }

        menu.style.left = `${Math.max(margin, Math.round(nextX))}px`;
        menu.style.top = `${Math.max(margin, Math.round(nextY))}px`;
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
            const canvas = node.closest('.designer-canvas');
            const scale = parseFloat(canvas?.dataset?.canvasScale || '1') || 1;
            node.style.left = Math.max(0, origLeft + ((e.clientX - startX) / scale)) + 'px';
            node.style.top  = Math.max(0, origTop  + ((e.clientY - startY) / scale)) + 'px';
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

// Data Model canvas — scoped pointer drag, pan, zoom, and viewport persistence.
window.schemaCanvasInterop = {
    _registrations: new Map(),

    init: (canvasId, dotNetRef, viewportX, viewportY) => {
        window.schemaCanvasInterop.dispose(canvasId);
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;

        const registration = {
            canvas,
            dotNetRef,
            draggingNode: null,
            panning: false,
            pointerId: null,
            tableName: '',
            startX: 0,
            startY: 0,
            originalLeft: 0,
            originalTop: 0,
            originalScrollLeft: 0,
            originalScrollTop: 0,
            pointerMoved: false,
            dragStarted: false,
            suppressClick: false,
            captureElement: null,
            viewportTimer: 0,
            frame: 0,
            pendingEdgeTable: null
        };

        const nodeForTable = (tableName) => Array.from(canvas.querySelectorAll('.schema-node'))
            .find(node => node.dataset.table === tableName);

        const anchor = (tableName, columnName, towardRight, laneFraction) => {
            const node = nodeForTable(tableName);
            if (!node) return null;
            const left = parseFloat(node.style.left) || 0;
            const top = parseFloat(node.style.top) || 0;
            const row = Array.from(node.querySelectorAll('[data-column]'))
                .find(candidate => candidate.dataset.column === columnName);
            const header = node.querySelector('.designer-table-node-header');
            const y = top + (row
                ? row.offsetTop + row.offsetHeight / 2
                : (header?.offsetHeight || 36) * (parseFloat(laneFraction || '0.5') || 0.5));
            return {
                x: towardRight ? left + node.offsetWidth : left,
                y,
                left,
                width: node.offsetWidth
            };
        };

        const connectorRoute = (start, end, nodeObstacles) => {
            const clearance = 18;
            const endpointStub = 24;
            const bendPenalty = 48;
            const outerLaneGap = 24;
            const epsilon = 0.001;
            const horizontal = 1;
            const vertical = 2;
            const expanded = nodeObstacles.map(obstacle => ({
                left: obstacle.left - clearance,
                top: obstacle.top - clearance,
                right: obstacle.right + clearance,
                bottom: obstacle.bottom + clearance
            }));
            const departure = {
                x: start.x + (start.side === 'right' ? endpointStub : -endpointStub),
                y: start.y
            };
            const approach = {
                x: end.x + (end.side === 'right' ? endpointStub : -endpointStub),
                y: end.y
            };
            const insideAny = point => expanded.some(obstacle =>
                point.x > obstacle.left + epsilon && point.x < obstacle.right - epsilon &&
                point.y > obstacle.top + epsilon && point.y < obstacle.bottom - epsilon);
            const segmentIsClear = (first, second) => {
                if (Math.abs(first.y - second.y) < epsilon) {
                    const left = Math.min(first.x, second.x);
                    const right = Math.max(first.x, second.x);
                    return expanded.every(obstacle =>
                        first.y <= obstacle.top + epsilon || first.y >= obstacle.bottom - epsilon ||
                        right <= obstacle.left + epsilon || left >= obstacle.right - epsilon);
                }
                if (Math.abs(first.x - second.x) < epsilon) {
                    const top = Math.min(first.y, second.y);
                    const bottom = Math.max(first.y, second.y);
                    return expanded.every(obstacle =>
                        first.x <= obstacle.left + epsilon || first.x >= obstacle.right - epsilon ||
                        bottom <= obstacle.top + epsilon || top >= obstacle.bottom - epsilon);
                }
                return false;
            };
            const simplify = rawPoints => {
                const result = [];
                rawPoints.forEach(point => {
                    const prior = result[result.length - 1];
                    if (prior && Math.abs(prior.x - point.x) < epsilon && Math.abs(prior.y - point.y) < epsilon)
                        return;
                    while (result.length >= 2) {
                        const first = result[result.length - 2];
                        const second = result[result.length - 1];
                        const collinear = Math.abs(first.x - second.x) < epsilon && Math.abs(second.x - point.x) < epsilon ||
                            Math.abs(first.y - second.y) < epsilon && Math.abs(second.y - point.y) < epsilon;
                        if (!collinear) break;
                        result.pop();
                    }
                    result.push(point);
                });
                return result;
            };
            const fallback = () => {
                const minTop = expanded.reduce((value, obstacle) => Math.min(value, obstacle.top), Math.min(departure.y, approach.y));
                const maxBottom = expanded.reduce((value, obstacle) => Math.max(value, obstacle.bottom), Math.max(departure.y, approach.y));
                const top = Math.max(4, minTop - outerLaneGap);
                const bottom = maxBottom + outerLaneGap;
                const routeY = Math.abs(departure.y - top) + Math.abs(approach.y - top) <=
                    Math.abs(departure.y - bottom) + Math.abs(approach.y - bottom) ? top : bottom;
                return [departure, { x: departure.x, y: routeY }, { x: approach.x, y: routeY }, approach];
            };

            let interior = null;
            if (!insideAny(departure) && !insideAny(approach)) {
                const xValues = [departure.x, approach.x, (departure.x + approach.x) / 2];
                const yValues = [departure.y, approach.y, (departure.y + approach.y) / 2];
                expanded.forEach(obstacle => {
                    xValues.push(obstacle.left, obstacle.right);
                    yValues.push(obstacle.top, obstacle.bottom);
                });
                const minX = Math.min(departure.x, approach.x, ...expanded.map(obstacle => obstacle.left));
                const maxX = Math.max(departure.x, approach.x, ...expanded.map(obstacle => obstacle.right));
                const minY = Math.min(departure.y, approach.y, ...expanded.map(obstacle => obstacle.top));
                const maxY = Math.max(departure.y, approach.y, ...expanded.map(obstacle => obstacle.bottom));
                xValues.push(Math.max(4, minX - outerLaneGap), maxX + outerLaneGap);
                yValues.push(Math.max(4, minY - outerLaneGap), maxY + outerLaneGap);
                const uniqueSorted = values => Array.from(new Set(values.map(value => Math.round(value * 1000) / 1000)))
                    .sort((left, right) => left - right);
                const xs = uniqueSorted(xValues);
                const ys = uniqueSorted(yValues);
                const points = [];
                const pointIndex = new Map();
                const pointKey = (x, y) => `${x}|${y}`;
                ys.forEach(y => xs.forEach(x => {
                    const point = { x, y };
                    if (insideAny(point)) return;
                    pointIndex.set(pointKey(x, y), points.length);
                    points.push(point);
                }));
                const startIndex = pointIndex.get(pointKey(departure.x, departure.y));
                const endIndex = pointIndex.get(pointKey(approach.x, approach.y));
                if (startIndex !== undefined && endIndex !== undefined) {
                    const adjacency = points.map(() => []);
                    const connect = (indices, direction) => {
                        indices.sort((left, right) => direction === horizontal
                            ? points[left].x - points[right].x
                            : points[left].y - points[right].y);
                        for (let index = 1; index < indices.length; index++) {
                            const prior = indices[index - 1];
                            const current = indices[index];
                            if (!segmentIsClear(points[prior], points[current])) continue;
                            const length = Math.abs(points[current].x - points[prior].x) + Math.abs(points[current].y - points[prior].y);
                            adjacency[prior].push({ target: current, direction, length });
                            adjacency[current].push({ target: prior, direction, length });
                        }
                    };
                    ys.forEach(y => connect(points.map((point, index) => point.y === y ? index : -1).filter(index => index >= 0), horizontal));
                    xs.forEach(x => connect(points.map((point, index) => point.x === x ? index : -1).filter(index => index >= 0), vertical));

                    const directionCount = 3;
                    const distance = new Array(points.length * directionCount).fill(Number.POSITIVE_INFINITY);
                    const previous = new Array(points.length * directionCount).fill(-1);
                    const heap = [];
                    let sequence = 0;
                    const heapPush = item => {
                        heap.push(item);
                        let index = heap.length - 1;
                        while (index > 0) {
                            const parent = Math.floor((index - 1) / 2);
                            const prior = heap[parent];
                            if (prior.cost < item.cost || prior.cost === item.cost && prior.sequence <= item.sequence) break;
                            heap[index] = prior;
                            index = parent;
                        }
                        heap[index] = item;
                    };
                    const heapPop = () => {
                        if (heap.length === 0) return null;
                        const result = heap[0];
                        const tail = heap.pop();
                        if (heap.length > 0) {
                            let index = 0;
                            while (true) {
                                let child = index * 2 + 1;
                                if (child >= heap.length) break;
                                if (child + 1 < heap.length) {
                                    const left = heap[child];
                                    const right = heap[child + 1];
                                    if (right.cost < left.cost || right.cost === left.cost && right.sequence < left.sequence)
                                        child++;
                                }
                                const candidate = heap[child];
                                if (candidate.cost > tail.cost || candidate.cost === tail.cost && candidate.sequence >= tail.sequence) break;
                                heap[index] = candidate;
                                index = child;
                            }
                            heap[index] = tail;
                        }
                        return result;
                    };
                    const startState = startIndex * directionCount + horizontal;
                    distance[startState] = 0;
                    heapPush({ state: startState, cost: 0, sequence: sequence++ });
                    while (heap.length > 0) {
                        const item = heapPop();
                        if (item.cost > distance[item.state] + epsilon) continue;
                        const currentPoint = Math.floor(item.state / directionCount);
                        const priorDirection = item.state % directionCount;
                        adjacency[currentPoint]
                            .sort((left, right) => left.direction - right.direction ||
                                points[left.target].x - points[right.target].x || points[left.target].y - points[right.target].y)
                            .forEach(edge => {
                                const candidate = item.cost + edge.length + (priorDirection === edge.direction ? 0 : bendPenalty);
                                const nextState = edge.target * directionCount + edge.direction;
                                if (candidate >= distance[nextState] - epsilon) return;
                                distance[nextState] = candidate;
                                previous[nextState] = item.state;
                                heapPush({ state: nextState, cost: candidate, sequence: sequence++ });
                            });
                    }
                    const horizontalEnd = endIndex * directionCount + horizontal;
                    const verticalEnd = endIndex * directionCount + vertical;
                    let endState = distance[horizontalEnd] <= distance[verticalEnd] + bendPenalty ? horizontalEnd : verticalEnd;
                    if (Number.isFinite(distance[endState])) {
                        const reversed = [];
                        for (let state = endState; state >= 0; state = previous[state]) {
                            reversed.push(points[Math.floor(state / directionCount)]);
                            if (state === startState) break;
                        }
                        interior = reversed.reverse();
                    }
                }
            }

            const points = simplify([{ x: start.x, y: start.y }, ...(interior || fallback()), { x: end.x, y: end.y }]);
            let longest = -1;
            let labelX = (start.x + end.x) / 2;
            let labelY = (start.y + end.y) / 2;
            for (let index = 1; index < points.length; index++) {
                const length = Math.abs(points[index].x - points[index - 1].x) + Math.abs(points[index].y - points[index - 1].y);
                if (length <= longest) continue;
                longest = length;
                labelX = (points[index].x + points[index - 1].x) / 2;
                labelY = (points[index].y + points[index - 1].y) / 2;
            }
            return { points, labelX, labelY };
        };

        const updateEdges = (tableName = null) => {
            registration.frame = 0;
            registration.pendingEdgeTable = null;
            const nodeObstacles = Array.from(canvas.querySelectorAll('.schema-node')).map(node => {
                const left = parseFloat(node.style.left) || 0;
                const top = parseFloat(node.style.top) || 0;
                return { left, top, right: left + node.offsetWidth, bottom: top + node.offsetHeight };
            });
            const visibleEdges = canvas.querySelectorAll('path[data-model-edge="visible"]');
            visibleEdges.forEach(edge => {
                if (tableName && edge.dataset.parentTable !== tableName && edge.dataset.childTable !== tableName) return;
                const parentNode = nodeForTable(edge.dataset.parentTable);
                const childNode = nodeForTable(edge.dataset.childTable);
                if (!parentNode || !childNode) return;
                const parentLeft = parseFloat(parentNode.style.left) || 0;
                const childLeft = parseFloat(childNode.style.left) || 0;
                const leftToRight = parentLeft + parentNode.offsetWidth / 2 <= childLeft + childNode.offsetWidth / 2;
                const parent = anchor(edge.dataset.parentTable, edge.dataset.parentColumn, leftToRight, edge.dataset.parentLane);
                const child = anchor(edge.dataset.childTable, edge.dataset.childColumn, !leftToRight, edge.dataset.childLane);
                if (!parent || !child) return;
                const route = connectorRoute(
                    { x: parent.x, y: parent.y, side: leftToRight ? 'right' : 'left' },
                    { x: child.x, y: child.y, side: leftToRight ? 'left' : 'right' },
                    nodeObstacles);
                let path = `M ${route.points[0].x.toFixed(1)},${route.points[0].y.toFixed(1)}`;
                for (let index = 1; index < route.points.length; index++) {
                    const previous = route.points[index - 1];
                    const current = route.points[index];
                    path += Math.abs(previous.y - current.y) < 0.001
                        ? ` H ${current.x.toFixed(1)}`
                        : ` V ${current.y.toFixed(1)}`;
                }
                edge.setAttribute('d', path);
                const hit = edge.parentElement?.querySelector('path[data-model-edge="hit"]');
                if (hit) hit.setAttribute('d', path);
                const labelBox = edge.parentElement?.querySelector('[data-model-label="box"]');
                const labelText = edge.parentElement?.querySelector('[data-model-label="text"]');
                if (labelBox) {
                    labelBox.setAttribute('x', (route.labelX - 111).toFixed(1));
                    labelBox.setAttribute('y', (route.labelY - 11).toFixed(1));
                }
                if (labelText) {
                    labelText.setAttribute('x', (route.labelX - 109).toFixed(1));
                    labelText.setAttribute('y', (route.labelY - 9).toFixed(1));
                }
            });
        };

        const scheduleEdgeUpdate = (tableName) => {
            registration.pendingEdgeTable = tableName;
            if (!registration.frame)
                registration.frame = requestAnimationFrame(() => updateEdges(registration.pendingEdgeTable));
        };

        const notifyViewport = () => {
            clearTimeout(registration.viewportTimer);
            registration.viewportTimer = setTimeout(() => {
                const scale = parseFloat(canvas.dataset.canvasScale || '1') || 1;
                registration.dotNetRef?.invokeMethodAsync(
                    'OnCanvasViewportChanged',
                    canvas.scrollLeft,
                    canvas.scrollTop,
                    scale);
            }, 250);
        };

        const setScale = (newScale, clientX, clientY) => {
            const stage = canvas.querySelector('.schema-canvas-stage');
            const viewport = canvas.querySelector('.schema-canvas-viewport');
            if (!stage || !viewport) return;
            const oldScale = parseFloat(canvas.dataset.canvasScale || '1') || 1;
            const bounded = Math.max(0.5, Math.min(2, newScale));
            const rect = canvas.getBoundingClientRect();
            const localX = clientX == null ? canvas.clientWidth / 2 : clientX - rect.left;
            const localY = clientY == null ? canvas.clientHeight / 2 : clientY - rect.top;
            const worldX = (canvas.scrollLeft + localX) / oldScale;
            const worldY = (canvas.scrollTop + localY) / oldScale;

            canvas.dataset.canvasScale = bounded.toString();
            stage.style.transform = `scale(${bounded})`;
            viewport.style.width = `${stage.offsetWidth * bounded}px`;
            viewport.style.height = `${stage.offsetHeight * bounded}px`;
            canvas.scrollLeft = Math.max(0, worldX * bounded - localX);
            canvas.scrollTop = Math.max(0, worldY * bounded - localY);
            notifyViewport();
        };

        registration.onPointerDown = (event) => {
            if (event.button !== 0) return;
            if (!(event.target instanceof Element)) return;
            const node = event.target.closest('.schema-node');
            const interactive = event.target.closest('button, a, input, select, textarea, [contenteditable="true"]');
            if (node && !interactive) {
                registration.draggingNode = node;
                registration.pointerId = event.pointerId;
                registration.tableName = node.dataset.table || '';
                registration.startX = event.clientX;
                registration.startY = event.clientY;
                registration.originalLeft = parseFloat(node.style.left) || 0;
                registration.originalTop = parseFloat(node.style.top) || 0;
                registration.pointerMoved = false;
                registration.dragStarted = false;
                registration.captureElement = null;
                return;
            }

            if (event.target.closest('.schema-node') || event.target.closest('[data-model-edge]')) return;
            event.preventDefault();
            canvas.focus({ preventScroll: true });
            registration.panning = true;
            registration.pointerId = event.pointerId;
            registration.startX = event.clientX;
            registration.startY = event.clientY;
            registration.pointerMoved = false;
            registration.originalScrollLeft = canvas.scrollLeft;
            registration.originalScrollTop = canvas.scrollTop;
            registration.captureElement = canvas;
            canvas.classList.add('is-panning');
            canvas.setPointerCapture?.(event.pointerId);
        };

        registration.onPointerMove = (event) => {
            if (registration.pointerId !== event.pointerId) return;
            if (Math.abs(event.clientX - registration.startX) > 4 || Math.abs(event.clientY - registration.startY) > 4)
                registration.pointerMoved = true;
            if (registration.draggingNode) {
                if (!registration.pointerMoved) return;
                event.preventDefault();
                if (!registration.dragStarted) {
                    registration.dragStarted = true;
                    registration.draggingNode.classList.add('is-dragging');
                    registration.captureElement = registration.draggingNode;
                    registration.draggingNode.setPointerCapture?.(event.pointerId);
                }
                const scale = parseFloat(canvas.dataset.canvasScale || '1') || 1;
                registration.draggingNode.style.left = Math.max(0, registration.originalLeft + (event.clientX - registration.startX) / scale) + 'px';
                registration.draggingNode.style.top = Math.max(0, registration.originalTop + (event.clientY - registration.startY) / scale) + 'px';
                scheduleEdgeUpdate(registration.tableName);
            } else if (registration.panning) {
                canvas.scrollLeft = registration.originalScrollLeft - (event.clientX - registration.startX);
                canvas.scrollTop = registration.originalScrollTop - (event.clientY - registration.startY);
            }
        };

        registration.onPointerUp = (event) => {
            if (registration.pointerId !== event.pointerId) return;
            if (registration.draggingNode) {
                const node = registration.draggingNode;
                const name = registration.tableName;
                const left = parseFloat(node.style.left) || 0;
                const top = parseFloat(node.style.top) || 0;
                registration.draggingNode = null;
                node.classList.remove('is-dragging');
                if (registration.dragStarted) {
                    registration.suppressClick = true;
                    setTimeout(() => { registration.suppressClick = false; }, 0);
                    registration.dotNetRef?.invokeMethodAsync('OnTableMoved', name, left, top);
                }
            }
            if (registration.panning) {
                registration.panning = false;
                canvas.classList.remove('is-panning');
                if (!registration.pointerMoved)
                    registration.dotNetRef?.invokeMethodAsync('OnCanvasSelectionCleared');
                notifyViewport();
            }
            const captureElement = registration.captureElement;
            registration.pointerId = null;
            registration.captureElement = null;
            registration.dragStarted = false;
            try { captureElement?.releasePointerCapture?.(event.pointerId); } catch { }
        };

        registration.onClickCapture = (event) => {
            if (!registration.suppressClick) return;
            registration.suppressClick = false;
            event.preventDefault();
            event.stopImmediatePropagation();
        };

        registration.onWheel = (event) => {
            event.preventDefault();
            const scale = parseFloat(canvas.dataset.canvasScale || '1') || 1;
            setScale(scale + (event.deltaY < 0 ? 0.1 : -0.1), event.clientX, event.clientY);
        };

        registration.onScroll = () => notifyViewport();
        registration.onKeyDown = (event) => {
            if (event.key !== 'Escape') return;
            event.preventDefault();
            event.stopPropagation();
            registration.dotNetRef?.invokeMethodAsync('OnCanvasSelectionCleared');
            canvas.focus({ preventScroll: true });
        };
        registration.setScale = setScale;
        registration.updateEdges = updateEdges;

        canvas.addEventListener('pointerdown', registration.onPointerDown);
        canvas.addEventListener('pointermove', registration.onPointerMove);
        canvas.addEventListener('pointerup', registration.onPointerUp);
        canvas.addEventListener('pointercancel', registration.onPointerUp);
        canvas.addEventListener('click', registration.onClickCapture, true);
        canvas.addEventListener('wheel', registration.onWheel, { passive: false });
        canvas.addEventListener('scroll', registration.onScroll, { passive: true });
        canvas.addEventListener('keydown', registration.onKeyDown);
        window.schemaCanvasInterop._registrations.set(canvasId, registration);

        requestAnimationFrame(() => {
            canvas.scrollLeft = Math.max(0, viewportX || 0);
            canvas.scrollTop = Math.max(0, viewportY || 0);
            updateEdges();
        });
    },

    fit: (canvasId) => {
        const registration = window.schemaCanvasInterop._registrations.get(canvasId);
        if (!registration) return;
        const { canvas } = registration;
        const stage = canvas.querySelector('.schema-canvas-stage');
        const nodes = Array.from(canvas.querySelectorAll('.schema-node'));
        if (!stage || nodes.length === 0) return;
        const bounds = nodes.reduce((current, node) => {
            const left = parseFloat(node.style.left) || 0;
            const top = parseFloat(node.style.top) || 0;
            return {
                minX: Math.min(current.minX, left),
                minY: Math.min(current.minY, top),
                maxX: Math.max(current.maxX, left + node.offsetWidth),
                maxY: Math.max(current.maxY, top + node.offsetHeight)
            };
        }, { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity });
        const availableWidth = Math.max(1, canvas.clientWidth - 40);
        const availableHeight = Math.max(1, canvas.clientHeight - 60);
        const modelWidth = Math.max(1, bounds.maxX - bounds.minX);
        const modelHeight = Math.max(1, bounds.maxY - bounds.minY);
        const scale = Math.max(0.5, Math.min(2, Math.min(availableWidth / modelWidth, availableHeight / modelHeight)));
        registration.setScale(scale, null, null);
        const modelCenterX = (bounds.minX + bounds.maxX) / 2;
        const modelCenterY = (bounds.minY + bounds.maxY) / 2;
        canvas.scrollLeft = Math.max(0, modelCenterX * scale - canvas.clientWidth / 2);
        canvas.scrollTop = Math.max(0, modelCenterY * scale - canvas.clientHeight / 2);
        return registration.dotNetRef?.invokeMethodAsync(
            'OnCanvasViewportChanged',
            canvas.scrollLeft,
            canvas.scrollTop,
            scale);
    },

    sync: (canvasId, viewportX, viewportY, scale) => {
        const registration = window.schemaCanvasInterop._registrations.get(canvasId);
        if (!registration) return;
        const { canvas } = registration;
        const stage = canvas.querySelector('.schema-canvas-stage');
        const viewport = canvas.querySelector('.schema-canvas-viewport');
        if (!stage || !viewport) return;
        const bounded = Math.max(0.5, Math.min(2, scale || 1));
        canvas.dataset.canvasScale = bounded.toString();
        stage.style.transform = `scale(${bounded})`;
        viewport.style.width = `${stage.offsetWidth * bounded}px`;
        viewport.style.height = `${stage.offsetHeight * bounded}px`;
        canvas.scrollLeft = Math.max(0, viewportX || 0);
        canvas.scrollTop = Math.max(0, viewportY || 0);
        registration.updateEdges();
    },

    dispose: (canvasId) => {
        const registration = window.schemaCanvasInterop._registrations.get(canvasId);
        if (!registration) return;
        const { canvas } = registration;
        canvas.removeEventListener('pointerdown', registration.onPointerDown);
        canvas.removeEventListener('pointermove', registration.onPointerMove);
        canvas.removeEventListener('pointerup', registration.onPointerUp);
        canvas.removeEventListener('pointercancel', registration.onPointerUp);
        canvas.removeEventListener('click', registration.onClickCapture, true);
        canvas.removeEventListener('wheel', registration.onWheel);
        canvas.removeEventListener('scroll', registration.onScroll);
        canvas.removeEventListener('keydown', registration.onKeyDown);
        clearTimeout(registration.viewportTimer);
        if (registration.frame) cancelAnimationFrame(registration.frame);
        window.schemaCanvasInterop._registrations.delete(canvasId);
    }
};

// Tablist key handling. Blazor receives the event and performs activation;
// this listener suppresses browser scrolling/default button behavior only for
// keys owned by the ARIA tab pattern. Tab and Shift+Tab remain untouched.
window.adminTabInterop = {
    _registrations: new Map(),

    register: (tabListId) => {
        const tabList = document.getElementById(tabListId);
        if (!tabList) return;

        const existing = window.adminTabInterop._registrations.get(tabListId);
        if (existing?.element === tabList) return;
        window.adminTabInterop.unregister(tabListId);

        const handler = (event) => {
            if (!(event.target instanceof Element) || !event.target.closest('[role="tab"]')) return;

            if (event.key === 'ArrowLeft' ||
                event.key === 'ArrowRight' ||
                event.key === 'Home' ||
                event.key === 'End' ||
                event.key === 'Delete' ||
                event.key === ' ' ||
                event.key === 'Spacebar') {
                event.preventDefault();
            }
        };

        tabList.addEventListener('keydown', handler);
        window.adminTabInterop._registrations.set(tabListId, { element: tabList, handler });
    },

    unregister: (tabListId) => {
        const registration = window.adminTabInterop._registrations.get(tabListId);
        if (!registration) return;

        registration.element.removeEventListener('keydown', registration.handler);
        window.adminTabInterop._registrations.delete(tabListId);
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
