import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('../../src/CSharpDB.Admin/wwwroot/js/modeler.js', import.meta.url), 'utf8');

function harness(width = 1200, height = 800) {
    const downloads = [], scheduled = [], revoked = [], removedSelectors = [];
    let output, serialized, decoded = false;
    class Element {
        constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.style = { setProperty(key, value) { this[key] = value; } }; this.dataset = {}; }
        setAttribute(key, value) { this.attrs[key] = value; }
        append(...children) { this.children.push(...children); }
        appendChild(child) { this.append(child); }
        remove() { this.removed = true; }
        click() { downloads.push({ name: this.download, url: this.href }); }
        querySelectorAll(selector) {
            if (selector.includes('[data-model-label]')) return [{ remove() { removedSelectors.push(selector); } }];
            return [];
        }
        cloneNode() { return new Element(this.tag); }
    }
    const original = new Element('svg');
    const stage = new Element('stage'); stage.style.width = `${width}px`; stage.style.height = `${height}px`;
    stage.querySelector = () => original;
    const canvas = new Element('canvas'); canvas.dataset.canvasInset = 64;
    canvas.querySelector = () => stage;
    const blobs = [];
    const context = { window: {}, Blob, encodeURIComponent,
        document: {
            body: new Element('body'),
            getElementById: id => id === 'model' ? canvas : null,
            createElementNS: (_, tag) => new Element(tag),
            createElement: tag => {
                const element = new Element(tag);
                if (tag === 'canvas') { output = element; element.getContext = () => ({ drawImage() {} }); element.toBlob = callback => callback(new Blob(['png'], { type: 'image/png' })); }
                return element;
            }
        },
        getComputedStyle: () => ({ backgroundColor: '#fff', getPropertyValue: key => key === 'marker-end' ? 'url("http://localhost/#crowfoot")' : '' }),
        XMLSerializer: class { serializeToString(svg) { serialized = svg; return '<svg xmlns="http://www.w3.org/2000/svg"/>'; } },
        Image: class { async decode() { assert.match(this.src, /^data:image\/svg\+xml/); decoded = true; } },
        URL: { createObjectURL(blob) { blobs.push(blob); return 'blob:download'; }, revokeObjectURL(url) { revoked.push(url); } },
        setTimeout(callback, delay) { scheduled.push({ callback, delay }); }
    };
    vm.runInNewContext(source, context);
    return { api: context.window.modelerFiles, downloads, scheduled, revoked, removedSelectors, blobs, original, stage,
        get output() { return output; }, get serialized() { return serialized; }, get decoded() { return decoded; } };
}

test('SVG export uses existing geometry and local marker URLs without changing the live diagram', async () => {
    const h = harness(), before = JSON.stringify(h.original);
    await h.api.exportCanvas('svg', 'model');
    assert.equal(h.downloads[0].name, 'data-model.svg');
    assert.equal(h.serialized.attrs.viewBox, '0 0 1200 800');
    const scene = h.serialized.children[1];
    assert.equal(scene.attrs.transform, 'translate(64,64)');
    assert.equal(scene.children[0].style['marker-end'], 'url(#crowfoot)');
    assert.equal(JSON.stringify(h.original), before);
    assert.equal(h.stage.style.width, '1200px');
    assert.equal(h.removedSelectors.length, 1);
    assert.match(h.removedSelectors[0], /data-model-label/);
    assert.equal(h.revoked.length, 0); h.scheduled[0].callback(); assert.equal(h.revoked.length, 1);
});

test('PNG export caps the output area and decodes a self-contained image', async () => {
    const h = harness(50000, 40000); await h.api.exportCanvas('png', 'model');
    assert.equal(h.downloads[0].name, 'data-model.png'); assert.equal(h.decoded, true);
    assert.ok(h.output.width <= 8192 && h.output.height <= 8192);
    assert.ok(h.output.width * h.output.height < 16010000);
    assert.equal(h.blobs[0].type, 'image/png');
});

test('unsupported formats and missing canvases report an error before downloading', async () => {
    const h = harness();
    await assert.rejects(h.api.exportCanvas('pdf', 'model'), /Unsupported/);
    await assert.rejects(h.api.exportCanvas('svg', 'missing'), /not available/);
    assert.equal(h.downloads.length, 0);
});

test('SQL export downloads the reviewed text without invoking any database operation', async () => {
    const h = harness(); h.api.downloadText('changes.sql', 'text/plain', 'BEGIN TRANSACTION;\nCOMMIT;');
    assert.equal(await h.blobs[0].text(), 'BEGIN TRANSACTION;\nCOMMIT;');
    assert.equal(h.downloads[0].name, 'changes.sql');
});
