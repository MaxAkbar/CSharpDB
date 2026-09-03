import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('../../src/CSharpDB.Admin/wwwroot/js/interop.js', import.meta.url), 'utf8');
const canvasSource = source.slice(source.indexOf('window.schemaCanvasInterop = {'), source.indexOf('// Tablist key handling.'));
const copy = value => JSON.parse(JSON.stringify(value));

function harness(layout = null, options = {}) {
    let document;
    class Element {
        constructor(tag = 'div', classes = '') {
            this.tagName = tag;
            this.dataset = {};
            this.style = {};
            this.children = [];
            this.parentElement = null;
            this.attributes = new Map();
            this.listeners = new Map();
            this.captured = new Set();
            this.classes = new Set(classes.split(' ').filter(Boolean));
            this.classList = {
                contains: value => this.classes.has(value),
                add: value => this.classes.add(value),
                remove: value => this.classes.delete(value),
                toggle: (value, force) => {
                    const enabled = force ?? !this.classes.has(value);
                    if (enabled) this.classes.add(value); else this.classes.delete(value);
                    return enabled;
                }
            };
        }
        get offsetWidth() { return this.fixedWidth ?? (parseFloat(this.style.width) || 0); }
        get offsetHeight() { return this.fixedHeight ?? (parseFloat(this.style.height) || 0); }
        get offsetTop() { return this.top || 0; }
        setAttribute(name, value) {
            this.attributes.set(name, String(value));
            if (name === 'class') this.classes = new Set(String(value).split(' '));
            if (name.startsWith('data-')) this.dataset[name.slice(5).replace(/-([a-z])/g, (_, letter) => letter.toUpperCase())] = String(value);
        }
        getAttribute(name) { return this.attributes.get(name) ?? null; }
        append(...children) { children.forEach(child => { child.parentElement = this; this.children.push(child); }); }
        replaceChildren(...children) { this.children.forEach(child => { child.parentElement = null; }); this.children = []; this.append(...children); }
        matches(selector) {
            if (selector.includes(',')) return selector.split(',').some(part => this.matches(part.trim()));
            const attribute = selector.match(/^(\w+)?\[([^=\]]+)(?:="([^"]*)")?\]$/);
            if (attribute) {
                if (attribute[1] && attribute[1] !== this.tagName) return false;
                const key = attribute[2];
                const value = key.startsWith('data-')
                    ? this.dataset[key.slice(5).replace(/-([a-z])/g, (_, letter) => letter.toUpperCase())]
                    : this.getAttribute(key);
                return attribute[3] == null ? value != null : value === attribute[3];
            }
            return selector.startsWith('.') ? this.classes.has(selector.slice(1)) : this.tagName === selector;
        }
        closest(selector) { for (let node = this; node; node = node.parentElement) if (node.matches(selector)) return node; return null; }
        querySelectorAll(selector) {
            const results = [];
            const walk = node => node.children.forEach(child => { if (child.matches(selector)) results.push(child); walk(child); });
            walk(this);
            return results;
        }
        querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
        addEventListener(name, handler) { this.listeners.set(name, handler); }
        removeEventListener(name, handler) { if (this.listeners.get(name) === handler) this.listeners.delete(name); }
        getBoundingClientRect() { return { left: 10, top: 20, width: this.clientWidth || this.offsetWidth, height: this.clientHeight || this.offsetHeight }; }
        focus() { document.activeElement = this; }
        setPointerCapture(id) { this.captured.add(id); }
        releasePointerCapture(id) { this.captured.delete(id); }
    }

    const canvas = new Element('div', 'schema-canvas');
    canvas.dataset.canvasScale = String(options.scale || 1);
    canvas.dataset.canvasInset = String(options.inset ?? 32);
    canvas.scrollLeft = 0; canvas.scrollTop = 0;
    canvas.clientWidth = 900; canvas.clientHeight = 500;
    const viewport = new Element('div', 'schema-canvas-viewport');
    const stage = new Element('div', 'schema-canvas-stage');
    stage.style.width = '1200px'; stage.style.height = '600px';
    const svg = new Element('svg', 'designer-canvas-svg');
    const group = new Element('g', 'schema-relationship selected');
    group.dataset.relationshipId = 'r1';
    group.dataset.connectorLayout = JSON.stringify(layout);
    const hit = new Element('path');
    const visible = new Element('path');
    [hit, visible].forEach((edge, index) => Object.assign(edge.dataset, {
        modelEdge: index ? 'visible' : 'hit', parentTable: 'parent', parentColumn: 'id', parentLane: '0.5',
        childTable: 'child', childColumn: 'id', childLane: '0.5'
    }));
    const handles = new Element('g'); handles.dataset.modelRouteHandles = '';
    group.append(hit, visible, handles); svg.append(group); stage.append(svg); viewport.append(stage); canvas.append(viewport);
    const node = (name, x, y) => {
        const value = new Element('div', 'schema-node');
        value.dataset.table = name; value.style.left = `${x}px`; value.style.top = `${y}px`;
        value.fixedWidth = 100; value.fixedHeight = 80;
        const header = new Element('div', 'designer-table-node-header'); header.fixedHeight = 36;
        value.append(header); stage.append(value); return value;
    };
    const parent = node('parent', 100, 100), child = node('child', 500, 260);
    document = { activeElement: canvas, getElementById: id => id === 'canvas' ? canvas : null, createElementNS: (_, tag) => new Element(tag) };
    const frames = new Map(); let nextFrame = 1;
    const calls = [];
    const context = vm.createContext({ window: { crypto: { randomUUID: () => `point-${nextFrame++}` } }, document, Element,
        requestAnimationFrame: callback => { const id = nextFrame++; frames.set(id, callback); return id; },
        cancelAnimationFrame: id => frames.delete(id), setTimeout: () => 1, clearTimeout: () => {}, console });
    vm.runInContext(canvasSource, context);
    const interop = context.window.schemaCanvasInterop;
    const dotNetRef = { invokeMethodAsync: async (method, ...args) => {
        calls.push({ method, args: copy(args) });
        if (method === 'OnConnectorLayoutChanged') {
            if (options.commit) return options.commit(args[1], { group, interop, canvas });
            group.dataset.connectorLayout = JSON.stringify(args[1]);
            interop.sync('canvas', canvas.scrollLeft, canvas.scrollTop, Number(canvas.dataset.canvasScale));
            return true;
        }
        return undefined;
    } };
    const flush = () => { const queued = [...frames.values()]; frames.clear(); queued.forEach(callback => callback()); };
    interop.init('canvas', dotNetRef, 0, 0); flush();
    const registration = interop._registrations.get('canvas');
    const event = (target, values = {}) => ({ target, button: 0, pointerId: 1, clientX: 0, clientY: 0,
        preventDefault() { this.prevented = true; }, stopPropagation() { this.stopped = true; }, stopImmediatePropagation() {}, ...values });
    const commits = () => calls.filter(call => call.method === 'OnConnectorLayoutChanged');
    const waypoint = () => handles.querySelector('[data-route-handle="waypoint"]');
    const segment = () => handles.querySelector('[data-route-handle="segment"]');
    const settle = async () => { await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); };
    const screenPoint = (x, y) => {
        const scale = Number(canvas.dataset.canvasScale), inset = Number(canvas.dataset.canvasInset);
        return { clientX: 10 + (x + inset) * scale - canvas.scrollLeft,
            clientY: 20 + (y + inset) * scale - canvas.scrollTop };
    };
    return { interop, registration, canvas, group, handles, hit, visible, parent, child, stage, viewport, svg,
        calls, commits, event, flush, frames, waypoint, segment, settle, screenPoint, document };
}

const manual = (points = [{ Id: 'bend-1', X: 300, Y: 50 }]) => ({ ParentSide: 1, ChildSide: 0, Waypoints: points });

test('custom routing visits ordered guides, preserves reversals, and falls back for invalid saved guides', () => {
    const { registration } = harness();
    const route = registration.connectorRoute({ x: 100, y: 100, side: 'right' }, { x: 500, y: 200, side: 'left' }, [],
        manual([{ Id: 'one', X: 200, Y: 40 }, { Id: 'two', X: 320, Y: 40 }, { Id: 'three', X: 260, Y: 40 }]));
    let cursor = -1;
    for (const guide of [{ x: 200, y: 40 }, { x: 320, y: 40 }, { x: 260, y: 40 }]) {
        cursor = route.points.findIndex((point, index) => index > cursor && point.x === guide.x && point.y === guide.y);
        assert.ok(cursor >= 0);
    }
    assert.equal(route.usedAutomaticFallback, false);
    const invalid = registration.connectorRoute({ x: 100, y: 100, side: 'right' }, { x: 500, y: 200, side: 'left' },
        [{ left: 250, top: 20, right: 350, bottom: 100 }], manual());
    assert.equal(invalid.usedAutomaticFallback, true);
    assert.ok(invalid.points.every(point => Number.isFinite(point.x) && Number.isFinite(point.y)));
});

test('tight-gap endpoint stubs are capped before an intervening physical card', () => {
    const { registration } = harness();
    const route = registration.connectorRoute({ x: 200, y: 140, side: 'right' }, { x: 210, y: 300, side: 'left' },
        [{ left: 100, top: 100, right: 200, bottom: 180 }, { left: 210, top: 100, right: 310, bottom: 340 }]);
    assert.equal(route.points[1].x, 205);
    assert.ok(route.points.every(point => Number.isFinite(point.x) && Number.isFinite(point.y)));
});

test('only selected connectors receive handles with 28 screen-pixel hit targets at every scale', () => {
    const h = harness(manual(), { scale: 0.5 });
    assert.equal(Number(h.waypoint().querySelector('.schema-route-handle-hit').getAttribute('r')), 28);
    h.interop.sync('canvas', 0, 0, 2);
    assert.equal(Number(h.waypoint().querySelector('.schema-route-handle-hit').getAttribute('r')), 7);
    h.group.classList.remove('selected'); h.registration.updateEdges();
    assert.equal(h.handles.children.length, 0);
});

test('pointer edits use model coordinates, coalesce routing, defer sync, and publish only on release', async () => {
    const h = harness(manual(), { scale: 2 });
    h.canvas.scrollLeft = 160; h.canvas.scrollTop = 80;
    const before = h.group.dataset.connectorLayout;
    const handle = h.waypoint();
    h.registration.onPointerDown(h.event(handle, { clientX: 450, clientY: 40 }));
    assert.ok(h.canvas.captured.has(1));
    h.registration.onPointerMove(h.event(handle, { clientX: 466, clientY: 56 }));
    h.registration.onPointerMove(h.event(handle, { clientX: 482, clientY: 72 }));
    assert.equal(h.frames.size, 1);
    assert.equal(h.commits().length, 0);
    assert.equal(h.group.dataset.connectorLayout, before);
    h.interop.sync('canvas', 0, 0, 0.5);
    assert.equal(h.canvas.dataset.canvasScale, '2');
    h.flush();
    assert.deepEqual(copy(h.registration.routeEdit.layout.Waypoints[0]), { Id: 'bend-1', X: 316, Y: 66 });
    h.registration.onPointerUp(h.event(handle, { clientX: 482, clientY: 72 })); await h.settle();
    assert.equal(h.commits().length, 1);
    assert.deepEqual(h.commits()[0].args[1].Waypoints[0], { Id: 'bend-1', X: 316, Y: 66 });
    assert.equal(h.canvas.captured.size, 0);
});

test('segment dragging creates one pinned-side waypoint perpendicular to the segment', async () => {
    const h = harness();
    const handle = h.segment();
    const route = h.registration.routes.get('r1');
    const index = Number(handle.dataset.segmentIndex);
    const first = route.points[index], second = route.points[index + 1];
    const horizontal = first.y === second.y;
    h.registration.onPointerDown(h.event(handle, { clientX: 350, clientY: 150 }));
    h.registration.onPointerMove(h.event(handle, { clientX: 370, clientY: 170 })); h.flush();
    const preview = copy(h.registration.routeEdit.layout);
    assert.equal(preview.ParentSide, 1); assert.equal(preview.ChildSide, 0);
    assert.equal(preview.Waypoints.length, 1);
    assert.equal(preview.Waypoints[0].X, (first.x + second.x) / 2 + (horizontal ? 0 : 20));
    assert.equal(preview.Waypoints[0].Y, (first.y + second.y) / 2 + (horizontal ? 20 : 0));
    h.registration.onPointerUp(h.event(handle)); await h.settle();
    assert.equal(h.commits().length, 1);
    assert.equal(h.commits()[0].args[1].Waypoints[0].Id, preview.Waypoints[0].Id);
});

test('Escape and pointercancel restore previews without notifying the model', () => {
    for (const cancel of ['escape', 'pointercancel']) {
        const h = harness(manual());
        const path = h.visible.getAttribute('d');
        const handle = h.waypoint();
        h.registration.onPointerDown(h.event(handle, { clientX: 300, clientY: 50 }));
        h.registration.onPointerMove(h.event(handle, { clientX: 340, clientY: 70 })); h.flush();
        if (cancel === 'escape') h.registration.onKeyDown(h.event(handle, { key: 'Escape' }));
        else h.registration.onPointerCancel(h.event(handle));
        assert.equal(h.visible.getAttribute('d'), path);
        assert.equal(h.commits().length, 0);
        assert.equal(h.registration.routeEdit, null);
        assert.equal(h.canvas.captured.size, 0);
    }
});

test('invalid pointer drops and rejected final callbacks restore the prior canonical route', async () => {
    const h = harness(manual());
    const original = h.visible.getAttribute('d');
    const handle = h.waypoint();
    h.registration.onPointerDown(h.event(handle, { clientX: 300, clientY: 50 }));
    h.registration.onPointerMove(h.event(handle, { clientX: 150, clientY: 140 })); h.flush();
    assert.ok(h.group.classList.contains('invalid'));
    h.registration.onPointerUp(h.event(handle)); await h.settle();
    assert.equal(h.commits().length, 0);
    assert.equal(h.visible.getAttribute('d'), original);

    const rejected = harness(manual(), { commit: async () => false });
    const before = rejected.group.dataset.connectorLayout;
    await rejected.interop.addBend('canvas', 'r1');
    assert.equal(rejected.commits().length, 1);
    assert.equal(rejected.group.dataset.connectorLayout, before);
    assert.equal(rejected.registration.previewLayouts.size, 0);
});

test('keyboard arrows use 8 or Shift-24 model units, publish on keyup, and Delete resets the last bend', async () => {
    const h = harness(manual());
    let handle = h.waypoint(); handle.focus();
    h.registration.onKeyDown(h.event(handle, { key: 'ArrowRight' }));
    h.registration.onKeyDown(h.event(handle, { key: 'ArrowRight', shiftKey: true, repeat: true }));
    assert.equal(h.commits().length, 0);
    h.flush();
    h.registration.onKeyUp(h.event(handle, { key: 'ArrowRight' })); await h.settle();
    assert.equal(h.commits()[0].args[1].Waypoints[0].X, 332);
    handle = h.waypoint(); h.registration.onFocusIn(h.event(handle));
    h.registration.onKeyDown(h.event(handle, { key: 'Delete' })); await h.settle();
    assert.equal(h.commits().at(-1).args[1], null);
    assert.equal(h.waypoint(), null);
});

test('add/remove APIs and double-click preserve guide order and do not mutate canonical attrs before acceptance', async () => {
    let resolve;
    const h = harness(null, { commit: () => new Promise(done => { resolve = done; }) });
    const pending = h.interop.addBend('canvas', 'r1');
    assert.equal(h.group.dataset.connectorLayout, 'null');
    assert.equal(h.commits().length, 1);
    resolve(false); await pending;
    assert.equal(h.group.dataset.connectorLayout, 'null');

    const accepted = harness(manual());
    const route = accepted.registration.routes.get('r1');
    const end = route.points.at(-2);
    accepted.registration.onDoubleClick(accepted.event(accepted.hit, accepted.screenPoint(end.x, end.y)));
    await accepted.settle();
    const layout = accepted.commits().at(-1).args[1];
    assert.equal(layout.Waypoints.length, 2);
    assert.equal(layout.Waypoints[0].Id, 'bend-1');
    await accepted.interop.removeBend('canvas', 'r1');
    assert.equal(accepted.commits().at(-1).args[1].Waypoints.length, 1);
});

test('route extents affect stage bounds and Fit Model; node drag, pan, zoom, focus and disposal remain wired', async () => {
    const h = harness(manual([{ Id: 'far', X: 1600, Y: 900 }]));
    assert.ok(parseFloat(h.stage.style.width) >= 1680);
    assert.ok(parseFloat(h.stage.style.height) >= 980);
    await h.interop.fit('canvas');
    assert.ok(Number(h.canvas.dataset.canvasScale) < 0.6);
    h.interop.sync('canvas', h.canvas.scrollLeft, h.canvas.scrollTop, 0.5);
    h.interop.focusConnector('canvas', 'r1'); assert.equal(h.document.activeElement, h.hit);
    h.registration.onPointerDown(h.event(h.parent, { clientX: 100, clientY: 100 }));
    h.registration.onPointerMove(h.event(h.parent, { clientX: 110, clientY: 120 }));
    h.registration.onPointerUp(h.event(h.parent));
    assert.ok(h.calls.some(call => call.method === 'OnTableMoved' && call.args[1] === 120 && call.args[2] === 140));
    h.registration.onWheel(h.event(h.canvas, { deltaY: -1, clientX: 100, clientY: 100 }));
    assert.equal(Number(h.canvas.dataset.canvasScale), 0.6);
    h.interop.dispose('canvas');
    assert.equal(h.canvas.listeners.size, 0);
    assert.equal(h.handles.children.length, 0);
    assert.equal(h.interop._registrations.size, 0);
});

test('fractional zoom and collapsed lanes retain exact routable coordinates', async () => {
    const h = harness(manual(), { scale: 1.1 });
    for (const edge of [h.hit, h.visible]) {
        edge.dataset.parentLane = String(1 / 8);
        edge.dataset.childLane = String(3 / 7);
    }
    h.registration.updateEdges();
    assert.equal(h.registration.routes.get('r1').usedAutomaticFallback, false);
    const handle = h.waypoint();
    h.registration.onPointerDown(h.event(handle, { clientX: 300, clientY: 50 }));
    h.registration.onPointerMove(h.event(handle, { clientX: 311, clientY: 61 })); h.flush();
    assert.equal(h.registration.routes.get('r1').usedAutomaticFallback, false);
    h.registration.onPointerUp(h.event(handle)); await h.settle();
    assert.equal(h.commits().length, 1);
    assert.ok(Math.abs(h.commits()[0].args[1].Waypoints[0].X - 310) < 0.000001);
    assert.ok(Math.abs(h.commits()[0].args[1].Waypoints[0].Y - 60) < 0.000001);
});

test('Add Bend and Enter focus new guides; removing the last guide focuses its connector', async () => {
    const h = harness();
    await h.interop.addBend('canvas', 'r1');
    assert.equal(h.document.activeElement, h.waypoint());
    await h.interop.removeBend('canvas', 'r1');
    assert.equal(h.document.activeElement, h.hit);
    const handle = h.segment(); handle.focus();
    h.registration.onKeyDown(h.event(handle, { key: 'Enter' })); await h.settle();
    assert.equal(h.commits().at(-1).args[1].Waypoints.length, 1);
    assert.equal(h.document.activeElement, h.waypoint());
});

test('a second pointer cannot replace an edit and only the edited route is refreshed in its frame', () => {
    const h = harness(manual());
    const untouched = { points: [], layout: null };
    h.registration.routes.set('other', untouched);
    const handle = h.waypoint();
    h.registration.onPointerDown(h.event(handle, { clientX: 300, clientY: 50 }));
    h.registration.onPointerDown(h.event(h.child, { pointerId: 2, clientX: 500, clientY: 260 }));
    assert.equal(h.registration.pointerId, 1);
    assert.equal(h.registration.draggingNode, null);
    h.registration.onPointerMove(h.event(handle, { clientX: 320, clientY: 60 })); h.flush();
    assert.equal(h.registration.routes.get('other'), untouched);
    h.registration.onPointerCancel(h.event(handle));
});

test('deselection clears cached handle identity so selecting the same guide notifies .NET again', () => {
    const h = harness(manual());
    h.registration.onFocusIn(h.event(h.waypoint()));
    const before = h.calls.filter(call => call.method === 'OnConnectorHandleSelected').length;
    h.group.classList.remove('selected'); h.registration.updateEdges();
    assert.equal(h.registration.selectedHandle, null);
    h.group.classList.add('selected'); h.registration.updateEdges();
    h.registration.onFocusIn(h.event(h.waypoint()));
    assert.equal(h.calls.filter(call => call.method === 'OnConnectorHandleSelected').length, before + 1);
});

test('the display inset is applied at init and sync without changing model coordinates or SVG dimensions', () => {
    const h = harness(manual(), { scale: 1.1 });
    assert.equal(h.stage.style.transform, 'scale(1.1) translate(32px, 32px)');
    assert.equal(parseFloat(h.viewport.style.width), (parseFloat(h.stage.style.width) + 64) * 1.1);
    assert.equal(parseFloat(h.viewport.style.height), (parseFloat(h.stage.style.height) + 64) * 1.1);
    assert.equal(Number(h.svg.getAttribute('width')), parseFloat(h.stage.style.width));
    h.canvas.scrollLeft = 180; h.canvas.scrollTop = 90;
    for (const point of [{ x: 0, y: 0 }, { x: -24, y: 118 }, { x: 300, y: 50 }]) {
        const converted = h.registration.modelPoint(h.screenPoint(point.x, point.y));
        assert.ok(Math.abs(converted.x - point.x) < 0.000001);
        assert.ok(Math.abs(converted.y - point.y) < 0.000001);
    }
    h.interop.sync('canvas', 180, 90, 2);
    assert.equal(h.stage.style.transform, 'scale(2) translate(32px, 32px)');
    assert.equal(parseFloat(h.viewport.style.width), (parseFloat(h.stage.style.width) + 64) * 2);
    assert.deepEqual(JSON.parse(h.group.dataset.connectorLayout), manual());
    assert.equal(h.parent.style.left, '100px');
});

test('zoom preserves the model point under the pointer with inset and fractional scale', () => {
    const h = harness(manual(), { scale: 1.1 });
    h.canvas.scrollLeft = 280; h.canvas.scrollTop = 170;
    const pointer = { clientX: 380, clientY: 220 };
    const before = h.registration.modelPoint(pointer);
    h.registration.setScale(1.7, pointer.clientX, pointer.clientY);
    const after = h.registration.modelPoint(pointer);
    assert.ok(Math.abs(after.x - before.x) < 0.000001);
    assert.ok(Math.abs(after.y - before.y) < 0.000001);
    assert.equal(h.stage.style.transform, 'scale(1.7) translate(32px, 32px)');
    assert.equal(parseFloat(h.viewport.style.height), (parseFloat(h.stage.style.height) + 64) * 1.7);
    assert.deepEqual(JSON.parse(h.group.dataset.connectorLayout), manual());
});

test('a pinned left endpoint at model X=0 keeps its negative stub visible after Fit Model', async () => {
    const layout = { ParentSide: 0, ChildSide: 0, Waypoints: [{ Id: 'guide', X: 300, Y: 50 }] };
    const h = harness(layout);
    h.parent.style.left = '0px'; h.registration.updateEdges();
    const route = h.registration.routes.get('r1');
    assert.equal(Math.min(...route.points.map(point => point.x)), -24);
    assert.equal(h.screenPoint(-24, 118).clientX - 10, 8);
    await h.interop.fit('canvas');
    const leftmostScreenX = Math.min(...route.points.map(point => h.screenPoint(point.x, point.y).clientX - 10));
    assert.ok(leftmostScreenX >= 0);
    assert.ok(leftmostScreenX < h.canvas.clientWidth);
    assert.equal(h.parent.style.left, '0px');
    assert.deepEqual(JSON.parse(h.group.dataset.connectorLayout), layout);
});

test('Fit Model centers inset route bounds and leaves saved positions invariant', async () => {
    const layout = manual([{ Id: 'far', X: 1600, Y: 900 }]);
    const h = harness(layout);
    const before = JSON.stringify({ parent: h.parent.style, child: h.child.style, layout: h.group.dataset.connectorLayout });
    await h.interop.fit('canvas');
    const bounds = h.registration.modelBounds, scale = Number(h.canvas.dataset.canvasScale);
    const centerX = (bounds.minX + bounds.maxX) / 2, centerY = (bounds.minY + bounds.maxY) / 2;
    assert.equal(h.canvas.scrollLeft, Math.max(0, (centerX + 32) * scale - h.canvas.clientWidth / 2));
    assert.equal(h.canvas.scrollTop, Math.max(0, (centerY + 32) * scale - h.canvas.clientHeight / 2));
    const model = h.registration.modelPoint(h.screenPoint(1600, 900));
    assert.ok(Math.abs(model.x - 1600) < 0.000001);
    assert.ok(Math.abs(model.y - 900) < 0.000001);
    assert.equal(JSON.stringify({ parent: h.parent.style, child: h.child.style, layout: h.group.dataset.connectorLayout }), before);
});
