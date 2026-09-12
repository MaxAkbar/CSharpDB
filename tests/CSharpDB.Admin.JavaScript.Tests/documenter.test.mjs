import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('../../src/CSharpDB.Admin/wwwroot/js/documenter.js', import.meta.url), 'utf8');
function harness() {
    const downloads = [], revoked = [], timers = [], calls = [];
    const root = {
        addEventListener(_, handler) { this.handler = handler; },
        removeEventListener(_, handler) { if (this.handler === handler) this.handler = null; },
        contains() { return true; }
    };
    const context = vm.createContext({
        WeakMap, Blob, setTimeout(fn) { timers.push(fn); },
        URL: { createObjectURL(blob) { downloads.push(blob); return 'blob:dictionary'; }, revokeObjectURL(url) { revoked.push(url); } },
        document: {
            body: { appendChild() {} },
            createElement() { return { click() { calls.push({ name: this.download, href: this.href }); }, remove() {} }; }
        }
    });
    vm.runInContext(source.replaceAll('export ', '') + '\nglobalThis.api = { attach, detach, download };', context);
    return { api: context.api, root, downloads, revoked, timers, calls };
}
test('document links navigate once and handlers detach on disposal', () => {
    const h = harness(), selected = [];
    const reference = { invokeMethodAsync(method, id) { selected.push([method, id]); return Promise.resolve(); } };
    h.api.attach(h.root, reference);
    h.api.attach(h.root, reference);
    let prevented = false;
    h.root.handler({ target: { closest() { return { getAttribute() { return '#object-abc'; } }; } }, preventDefault() { prevented = true; } });
    assert.equal(prevented, true);
    assert.deepEqual(selected, [['SelectAnchor', 'object-abc']]);
    h.api.detach(h.root);
    assert.equal(h.root.handler, null);
});
test('stream downloads preserve UTF-8 bytes and defer blob disposal', async () => {
    const h = harness(), bytes = new TextEncoder().encode('<html>λ café</html>');
    await h.api.download('database-dictionary.html', 'text/html;charset=utf-8', { async arrayBuffer() { return bytes.buffer; } });
    assert.equal(await h.downloads[0].text(), '<html>λ café</html>');
    assert.equal(h.calls[0].name, 'database-dictionary.html');
    assert.deepEqual(h.revoked, []);
    h.timers[0]();
    assert.deepEqual(h.revoked, ['blob:dictionary']);
});
