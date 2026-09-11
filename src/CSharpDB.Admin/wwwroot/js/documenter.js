const handlers = new WeakMap();
export function attach(root, reference) {
    detach(root);
    const handler = event => {
        const link = event.target.closest('a[href^="#object-"]');
        if (!link || !root.contains(link)) return;
        event.preventDefault();
        reference.invokeMethodAsync('SelectAnchor', link.getAttribute('href').slice(1));
    };
    root.addEventListener('click', handler);
    handlers.set(root, handler);
}
export function detach(root) {
    const handler = handlers.get(root);
    if (handler) root.removeEventListener('click', handler);
    handlers.delete(root);
}
export function resetDetailScroll(root) {
    const pane = root.querySelector('.detail-pane');
    if (pane) pane.scrollTop = 0;
}
export async function download(name, type, stream) {
    const bytes = await stream.arrayBuffer();
    const url = URL.createObjectURL(new Blob([bytes], { type }));
    try {
        const link = document.createElement('a');
        link.href = url;
        link.download = name;
        document.body.appendChild(link);
        link.click();
        link.remove();
    } finally {
        // Let web browsers and the desktop WebView start consuming the blob.
        setTimeout(() => URL.revokeObjectURL(url), 1000);
    }
}
