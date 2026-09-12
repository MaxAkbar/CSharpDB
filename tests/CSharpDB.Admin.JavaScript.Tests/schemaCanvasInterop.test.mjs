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
    let label, tooltip, labelButton;
    if (options.labels) {
        const layer = new Element('div', 'schema-relationship-label-layer');
        label = new Element('div', 'schema-relationship-label'); label.dataset.modelLabelId = 'r1';
        label.fixedWidth = 160; label.fixedHeight = 24;
        labelButton = new Element('button', 'schema-relationship-label-button');
        tooltip = new Element('div', 'schema-relationship-tooltip'); tooltip.fixedWidth = 300; tooltip.fixedHeight = 80;
        label.append(labelButton, tooltip); layer.append(label); stage.append(layer);
    }
    let tableGroup, groupTitle;
    if (options.grouped) {
        parent.dataset.groupId = 'g1';
        if (options.grouped !== 'parent-only') child.dataset.groupId = 'g1';
        tableGroup = new Element('div', 'schema-table-group'); tableGroup.dataset.groupId = 'g1';
        groupTitle = new Element('button', 'schema-group-title'); groupTitle.dataset.groupTitle = 'g1';
        tableGroup.append(groupTitle); stage.append(tableGroup);
    }
    document = { activeElement: canvas, getElementById: id => id === 'canvas' ? canvas : null, createElementNS: (_, tag) => new Element(tag) };
    const frames = new Map(), timers = new Map(); let nextFrame = 1;
    const calls = [];
    const context = vm.createContext({ window: { crypto: { randomUUID: () => `point-${nextFrame++}` } }, document, Element,
        requestAnimationFrame: callback => { const id = nextFrame++; frames.set(id, callback); return id; },
        cancelAnimationFrame: id => frames.delete(id),
        setTimeout: callback => { const id = nextFrame++; timers.set(id, callback); return id; }, clearTimeout: id => timers.delete(id), console });
    vm.runInContext(canvasSource, context);
    const interop = context.window.schemaCanvasInterop;
    const dotNetRef = { invokeMethodAsync: async (method, ...args) => {
        calls.push({ method, args: copy(args) });
        if (method === 'OnTableGroupMoved') {
            if (options.groupCommit === false) return false;
            const saved = JSON.parse(group.dataset.connectorLayout);
            if (saved && parent.dataset.groupId === args[0] && child.dataset.groupId === args[0]) {
                saved.Waypoints.forEach(point => { point.X += args[1]; point.Y += args[2]; });
                group.dataset.connectorLayout = JSON.stringify(saved);
            }
            interop.sync('canvas', canvas.scrollLeft, canvas.scrollTop, Number(canvas.dataset.canvasScale));
            return true;
        }
        if (method === 'OnConnectorLayoutChanged') {
            if (options.commit) return options.commit(args[1], { group, interop, canvas });
            group.dataset.connectorLayout = JSON.stringify(args[1]);
            interop.sync('canvas', canvas.scrollLeft, canvas.scrollTop, Number(canvas.dataset.canvasScale));
            return true;
        }
        return undefined;
    } };
    const flush = () => { const queued = [...frames.values()]; frames.clear(); queued.forEach(callback => callback()); };
    const flushTimers = () => { const queued = [...timers.values()]; timers.clear(); queued.forEach(callback => callback()); };
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
        calls, commits, event, flush, flushTimers, frames, waypoint, segment, settle, screenPoint, document, node, tableGroup, groupTitle, label, tooltip, labelButton };
}

test('moving a separate related table pair leaves unrelated connectors fixed during preview, sync, and reopen', async () => {
    const h = harness(null, { scale: 1.25 });
    const products = h.node('products', 300, 600);
    h.node('inventory', 750, 500);
    const relationship = h.document.createElementNS('', 'g');
    relationship.setAttribute('class', 'schema-relationship');
    Object.assign(relationship.dataset, { relationshipId: 'products-inventory', connectorLayout: 'null' });
    for (const kind of ['hit', 'visible']) {
        const edge = h.document.createElementNS('', 'path');
        Object.assign(edge.dataset, h.visible.dataset, { modelEdge: kind, parentTable: 'products', childTable: 'inventory' });
        relationship.append(edge);
    }
    h.svg.append(relationship);
    h.registration.updateEdges();
    const original = copy(h.registration.routes.get('r1').points);
    const connected = copy(h.registration.routes.get('products-inventory').points);
    const header = products.children[0];
    h.registration.onPointerDown(h.event(header));
    for (const clientX of [25, 100, -75]) {
        h.registration.onPointerMove(h.event(header, { clientX, clientY: 50 })); h.flush();
        assert.deepEqual(copy(h.registration.routes.get('r1').points), original, 'An unrelated bend must not follow products');
        assert.notDeepEqual(copy(h.registration.routes.get('products-inventory').points), connected, 'Connected endpoints must follow products');
    }
    h.registration.onPointerUp(h.event(header)); await h.settle();
    h.interop.sync('canvas', 0, 0, 1.25); h.flush();
    assert.deepEqual(copy(h.registration.routes.get('r1').points), original, 'The final render must not move unrelated bends either');
    assert.equal(h.calls.filter(call => call.method === 'OnTableMoved').length, 1);
    assert.equal(h.commits().length, 0, 'Moving tables must not rewrite connector layouts');
    const reopened = harness();
    reopened.node('products', parseFloat(products.style.left), parseFloat(products.style.top));
    reopened.node('inventory', 750, 500);
    reopened.registration.updateEdges();
    assert.deepEqual(copy(reopened.registration.routes.get('r1').points), original);
});

const manual = (points = [{ Id: 'bend-1', X: 300, Y: 50 }]) => ({ ParentSide: 1, ChildSide: 0, FollowEndpointRows: false, Waypoints: points });

const middleLane = () => ({ ...manual([{ Id: 'upper', X: 350, Y: 118 }, { Id: 'lower', X: 350, Y: 278 }]), FollowEndpointRows: true });

for (const layout of [null, middleLane()]) {
    test(`an unrelated table only reroutes an ${layout ? 'edited' : 'automatic'} connector when it becomes an obstacle`, () => {
        const h = harness(layout);
        const blocker = h.node('blocker', 300, 600);
        h.registration.updateEdges();
        const original = copy(h.registration.routes.get('r1').points);
        const stored = h.group.dataset.connectorLayout;
        h.registration.onPointerDown(h.event(blocker.children[0]));
        h.registration.onPointerMove(h.event(blocker, { clientY: -500 })); h.flush();
        const route = h.registration.routes.get('r1');
        assert.notDeepEqual(copy(route.points), original, 'A table moved into the path must still be avoided');
        for (let index = 1; index < route.points.length; index++) {
            const a = route.points[index - 1], b = route.points[index];
            const intersects = a.y === b.y
                ? a.y > 100 && a.y < 180 && Math.max(a.x, b.x) > 300 && Math.min(a.x, b.x) < 400
                : a.x > 300 && a.x < 400 && Math.max(a.y, b.y) > 100 && Math.min(a.y, b.y) < 180;
            assert.equal(intersects, false, 'No connector leg may pass through the moved card');
        }
        assert.equal(route.usedAutomaticFallback, layout != null);
        assert.equal(h.group.dataset.connectorLayout, stored, 'Obstacle avoidance must not rewrite saved guides');
        h.registration.onPointerCancel(h.event(blocker));
        assert.deepEqual(copy(h.registration.routes.get('r1').points), original);
        assert.equal(h.calls.filter(call => call.method === 'OnTableMoved').length, 0);
    });
}

for (const table of ['parent', 'child']) {
    test(`sliding then moving the ${table} keeps one middle segment through save, reopen, and another slide`, async () => {
        const h = harness(null, { scale: 1.25 });
        const grip = h.segment();
        h.registration.onPointerDown(h.event(grip));
        h.registration.onPointerMove(h.event(grip, { clientX: 25 })); h.flush();
        h.registration.onPointerUp(h.event(grip)); await h.settle();
        const saved = JSON.parse(h.group.dataset.connectorLayout), lane = saved.Waypoints[0].X;
        assert.equal(saved.FollowEndpointRows, true);
        const target = h[table].children[0];
        h.registration.onPointerDown(h.event(target));
        h.registration.onPointerMove(h.event(target, { clientX: 40, clientY: 75 })); h.flush();
        const route = copy(h.registration.routes.get('r1'));
        assert.equal(route.points.length, 4, 'Moving a table must not add a dogleg');
        assert.deepEqual(route.points, [{ x: route.start.x, y: route.start.y }, { x: lane, y: route.start.y },
            { x: lane, y: route.end.y }, { x: route.end.x, y: route.end.y }]);
        h.registration.onPointerUp(h.event(target)); await h.settle();
        assert.equal(h.calls.filter(call => call.method === 'OnTableMoved').length, 1);
        assert.equal(h.commits().length, 1, 'Table movement needs no separate connector save');
        assert.deepEqual(JSON.parse(h.group.dataset.connectorLayout), saved, 'Requested lane stays unchanged');
        const reopened = harness(saved);
        Object.assign(reopened[table].style, h[table].style); reopened.registration.updateEdges();
        assert.deepEqual(copy(reopened.registration.routes.get('r1').points), route.points);
        reopened.registration.onKeyDown(reopened.event(reopened.segment(), { key: 'ArrowRight' }));
        reopened.registration.onKeyUp(reopened.event(reopened.segment(), { key: 'ArrowRight' })); await reopened.settle();
        assert.equal(reopened.registration.routes.get('r1').points.length, 4);
        assert.equal(reopened.commits().at(-1).args[1].FollowEndpointRows, true);
    });
}

test('middle lanes follow row anchors, clamp at a moved table, and restore on canceled moves', () => {
    const h = harness(middleLane());
    const original = copy(h.registration.routes.get('r1').points);
    h.registration.onPointerDown(h.event(h.parent.children[0]));
    h.registration.onPointerMove(h.event(h.parent, { clientX: 170, clientY: 40 })); h.flush();
    const moved = h.registration.routes.get('r1');
    assert.equal(moved.points.length, 4);
    assert.equal(moved.points[1].x, moved.departure.x, 'Lane must stay outside the moved table');
    assert.equal(moved.layout.Waypoints[0].Y, moved.start.y, 'Bend handles follow the effective route');
    h.registration.onPointerCancel(h.event(h.parent));
    assert.deepEqual(copy(h.registration.routes.get('r1').points), original);
    assert.equal(h.calls.filter(call => call.method === 'OnTableMoved').length, 0);
    // Header lane changes simulate collapsed/detail-mode anchors.
    h.visible.dataset.parentLane = '0.75'; h.registration.updateEdges();
    assert.equal(h.registration.routes.get('r1').points[1].y, 127);
    assert.equal(h.registration.routes.get('r1').points.length, 4);
});

test('legacy two-corner slides recover from old row coordinates but explicit bends remain fixed', () => {
    const legacy = middleLane(); delete legacy.FollowEndpointRows;
    legacy.Waypoints[0].Y = 90; legacy.Waypoints[1].Y = 320;
    const h = harness(legacy);
    assert.equal(h.registration.routes.get('r1').points.length, 4);
    const fixed = harness({ ...legacy, FollowEndpointRows: false });
    assert.deepEqual(copy(fixed.registration.routes.get('r1').layout.Waypoints), legacy.Waypoints);
    assert.ok(fixed.registration.routes.get('r1').points.length > 4);
});

test('explicit bend editing starts at the moved endpoint rows and opts out of lane following', async () => {
    for (const action of ['add', 'remove', 'move']) {
        const h = harness(middleLane()); h.parent.style.top = '140px'; h.registration.updateEdges();
        if (action === 'add') await h.interop.addBend('canvas', 'r1');
        else if (action === 'remove') await h.interop.removeBend('canvas', 'r1');
        else {
            const grip = h.waypoint(); h.registration.onPointerDown(h.event(grip));
            h.registration.onPointerMove(h.event(grip, { clientX: 16 })); h.flush();
            h.registration.onPointerUp(h.event(grip)); await h.settle();
        }
        const saved = h.commits().at(-1).args[1];
        assert.equal(saved.FollowEndpointRows, false);
        assert.equal(saved.Waypoints[0].Y, 158);
    }
});

for (const grouped of [true, 'parent-only']) {
    test(`group moves retain a simple lane (${grouped})`, async () => {
        const h = harness(middleLane(), { grouped });
        h.registration.onPointerDown(h.event(h.groupTitle));
        h.registration.onPointerMove(h.event(h.groupTitle, { clientX: 40, clientY: 50 })); h.flush();
        assert.equal(h.registration.routes.get('r1').points.length, 4);
        assert.equal(h.registration.routes.get('r1').points[1].x, grouped === true ? 390 : 350);
        h.registration.onPointerUp(h.event(h.groupTitle)); await h.settle();
        assert.equal(h.registration.routes.get('r1').points.length, 4);
        assert.equal(JSON.parse(h.group.dataset.connectorLayout).FollowEndpointRows, true);
    });
}

const intersects = (a, b) => a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top;
const labelBounds = label => ({ left: parseFloat(label.style.left), top: parseFloat(label.style.top),
    right: parseFloat(label.style.left) + label.offsetWidth, bottom: parseFloat(label.style.top) + label.offsetHeight });

test('relationship labels avoid both endpoint cards, group titles, and every connector segment in a tight gap', () => {
    const h = harness();
    const points = [{ x: 220, y: 74 }, { x: 302, y: 74 }, { x: 302, y: 160 }, { x: 380, y: 160 }];
    const cards = [{ left: 0, top: 0, right: 220, bottom: 116 }, { left: 380, top: 60, right: 600, bottom: 206 }];
    const title = { left: 0, top: -40, right: 600, bottom: -12 };
    const segments = points.slice(1).map((point, index) => [points[index], point]);
    segments.push([{ x: 230, y: 245 }, { x: 600, y: 245 }]);
    const view = { left: -48, top: -48, right: 800, bottom: 460 };
    const obstacles = [...cards, title];
    const before = copy({ points, obstacles });
    for (const [width, height] of [[160, 24], [300, 90]]) {
        const result = h.interop.placeRelationshipLabel(points, width, height, obstacles, segments, view);
        assert.ok(result, 'A clear placement should be found');
        for (const rect of obstacles) assert.ok(!intersects(result, rect));
        for (const [a, b] of segments) assert.ok(!intersects(result, {
            left: Math.min(a.x, b.x) - 2, top: Math.min(a.y, b.y) - 2,
            right: Math.max(a.x, b.x) + 2, bottom: Math.max(a.y, b.y) + 2 }));
        assert.deepEqual(copy(result), copy(h.interop.placeRelationshipLabel(points, width, height, [...obstacles].reverse(), segments, view)));
    }
    assert.deepEqual(copy({ points, obstacles }), before);
});

test('label placement stays in fractional-zoom viewports and declines a fully occupied viewport', () => {
    const h = harness();
    const points = [{ x: 200, y: 130 }, { x: 500, y: 130 }];
    for (const scale of [0.5, 1, 1.25, 2]) {
        const view = { left: 50, top: 20, right: 50 + 800 / scale, bottom: 20 + 500 / scale };
        const result = h.interop.placeRelationshipLabel(points, 160, 24, [], [[points[0], points[1]]], view);
        assert.ok(result);
        assert.ok(result.left >= view.left && result.right <= view.right && result.top >= view.top && result.bottom <= view.bottom);
        assert.equal(h.interop.placeRelationshipLabel(points, 160, 24, [view], [], view), null);
    }
});

test('foreground labels expose a full popup on hover/focus, accept clicks without panning, and dismiss with Escape', () => {
    const h = harness(null, { labels: true });
    assert.ok(h.label.classList.contains('is-visible'));
    assert.ok(!h.label.classList.contains('is-expanded'));
    h.registration.onPointerOver(h.event(h.hit));
    assert.ok(h.label.classList.contains('is-expanded'));
    const before = copy(h.registration.modelBounds);
    h.registration.onPointerDown(h.event(h.labelButton));
    assert.equal(h.registration.panning, false);
    assert.equal(h.registration.pointerId, null);
    h.registration.onPointerOut(h.event(h.hit, { relatedTarget: h.canvas }));
    h.registration.onPointerOver(h.event(h.tooltip)); h.flushTimers();
    assert.ok(h.label.classList.contains('is-expanded'), 'Popup stays available while the pointer crosses the gap');
    h.registration.onPointerOut(h.event(h.tooltip, { relatedTarget: h.canvas })); h.flushTimers();
    assert.ok(!h.label.classList.contains('is-expanded'));
    h.registration.onPointerOver(h.event(h.hit));
    h.registration.onPointerOut(h.event(h.hit, { relatedTarget: h.parent }));
    h.parent.focus(); h.registration.onFocusOut(); h.flushTimers();
    assert.ok(!h.label.classList.contains('is-expanded'), 'Focus leaving the connector must not cancel hover dismissal');
    h.hit.focus(); h.registration.onFocusIn(h.event(h.hit));
    assert.ok(h.label.classList.contains('is-expanded'));
    h.registration.onKeyDown(h.event(h.hit, { key: 'Escape' }));
    assert.ok(!h.label.classList.contains('is-expanded'));
    assert.deepEqual(copy(h.registration.modelBounds), before, 'Transient labels never change Fit Model extents');
    h.group.classList.remove('selected'); h.canvas.focus(); h.registration.onFocusOut(); h.flushTimers();
    assert.ok(!h.label.classList.contains('is-visible'));
    assert.equal(h.commits().length, 0);
    h.interop.dispose('canvas');
    assert.ok(!h.canvas.listeners.has('pointerover') && !h.canvas.listeners.has('focusout'));
});

test('labels track drag previews and canceled moves without changing stored connector guides', () => {
    const h = harness(manual(), { labels: true, grouped: true, scale: 1.25 });
    const original = copy(labelBounds(h.label)), saved = h.group.dataset.connectorLayout;
    h.registration.onPointerDown(h.event(h.parent.children[0]));
    h.registration.onPointerMove(h.event(h.parent.children[0], { clientX: 120, clientY: 50 })); h.flush();
    if (h.label.classList.contains('is-visible')) {
        assert.ok(!intersects(labelBounds(h.label), labelBounds(h.parent)));
        assert.ok(!intersects(labelBounds(h.label), labelBounds(h.child)));
    }
    assert.equal(h.group.dataset.connectorLayout, saved);
    h.registration.onPointerCancel(h.event(h.parent));
    assert.deepEqual(copy(labelBounds(h.label)), original);
    assert.equal(h.commits().length, 0);
});

for (const pointerType of ['touch', 'pen']) {
    test(`${pointerType} moves a table, group, and connector once and cancellation restores geometry`, async () => {
        for (const targetKind of ['table', 'group', 'connector']) {
            const h = harness(manual(), { grouped: true, scale: 1.25 });
            let target = targetKind === 'table' ? h.parent.children[0] : targetKind === 'group' ? h.groupTitle : h.waypoint();
            const start = h.screenPoint(300, 50);
            const event = values => h.event(target, { pointerType, isPrimary: true, ...start, ...values });
            const moved = { clientX: start.clientX + 50, clientY: start.clientY + (targetKind === 'connector' ? -25 : 25) };
            const before = [h.parent.style.left, h.child.style.left, h.group.dataset.connectorLayout];
            h.registration.onPointerDown(event());
            h.registration.onPointerMove(event(moved)); h.flush();
            h.registration.onPointerCancel(event());
            assert.deepEqual([h.parent.style.left, h.child.style.left, h.group.dataset.connectorLayout], before);
            assert.equal(h.canvas.captured.size, 0);
            if (targetKind === 'connector') target = h.waypoint();
            h.registration.onPointerDown(event());
            h.registration.onPointerMove(event(moved)); h.flush();
            h.registration.onPointerUp(event()); await h.settle();
            const method = targetKind === 'table' ? 'OnTableMoved' : targetKind === 'group' ? 'OnTableGroupMoved' : 'OnConnectorLayoutChanged';
            assert.equal(h.calls.filter(call => call.method === method).length, 1, `${pointerType}: ${targetKind}`);
            assert.equal(h.canvas.captured.size, 0);
        }
    });
}

test('group drag translates members and internal guides at fractional zoom, persists once and defers sync', async () => {
    const h = harness(manual(), { grouped: true, inset: 64, scale: 1.25 });
    const start = h.screenPoint(100, 65);
    h.registration.onPointerDown(h.event(h.groupTitle, start));
    h.registration.onPointerMove(h.event(h.groupTitle, { clientX: start.clientX + 50, clientY: start.clientY + 25 }));
    h.registration.onPointerMove(h.event(h.groupTitle, { clientX: start.clientX + 100, clientY: start.clientY + 50 }));
    assert.equal(h.frames.size, 1);
    assert.equal(h.calls.filter(call => call.method === 'OnTableGroupMoved').length, 0);
    assert.equal(h.parent.style.left, '180px'); assert.equal(h.child.style.left, '580px');
    assert.equal(h.registration.previewLayouts.get('r1').Waypoints[0].X, 380);
    assert.equal(JSON.parse(h.group.dataset.connectorLayout).Waypoints[0].X, 300);
    h.interop.sync('canvas', 0, 0, 0.5);
    assert.equal(h.canvas.dataset.canvasScale, '1.25');
    h.flush();
    assert.equal(h.tableGroup.style.left, '164px');
    h.registration.onPointerUp(h.event(h.groupTitle)); await h.settle();
    assert.deepEqual(h.calls.filter(call => call.method === 'OnTableGroupMoved')[0].args, ['g1', 80, 40]);
    assert.equal(h.calls.filter(call => call.method === 'OnTableGroupMoved').length, 1);
    assert.equal(JSON.parse(h.group.dataset.connectorLayout).Waypoints[0].X, 380);
    assert.equal(h.canvas.captured.size, 0);
});

test('group drag keeps crossing connector bends fixed and does not infer membership', async () => {
    const h = harness(manual(), { grouped: 'parent-only' });
    h.registration.onPointerDown(h.event(h.groupTitle));
    h.registration.onPointerMove(h.event(h.groupTitle, { clientX: 40, clientY: 40 })); h.flush();
    assert.equal(h.registration.previewLayouts.size, 0);
    h.registration.onPointerUp(h.event(h.groupTitle)); await h.settle();
    assert.equal(h.parent.style.left, '140px'); assert.equal(h.child.style.left, '500px');
    assert.equal(JSON.parse(h.group.dataset.connectorLayout).Waypoints[0].X, 300);
    assert.equal(h.child.dataset.groupId, undefined);
});

test('group Escape, pointercancel and rejected callbacks restore all coordinates and guides', async () => {
    for (const action of ['escape', 'cancel', 'reject']) {
        const h = harness(manual(), { grouped: true, groupCommit: action !== 'reject' });
        h.registration.onPointerDown(h.event(h.groupTitle));
        h.registration.onPointerMove(h.event(h.groupTitle, { clientX: 40, clientY: 40 })); h.flush();
        if (action === 'escape') h.registration.onKeyDown(h.event(h.groupTitle, { key: 'Escape' }));
        else if (action === 'cancel') h.registration.onPointerCancel(h.event(h.groupTitle));
        else { h.registration.onPointerUp(h.event(h.groupTitle)); await h.settle(); }
        assert.equal(h.parent.style.left, '100px'); assert.equal(h.child.style.left, '500px');
        assert.equal(JSON.parse(h.group.dataset.connectorLayout).Waypoints[0].X, 300);
        assert.equal(h.registration.previewLayouts.size, 0);
        assert.equal(h.registration.groupEdit, null);
        assert.equal(h.canvas.captured.size, 0);
        assert.equal(h.calls.filter(call => call.method === 'OnTableGroupMoved').length, action === 'reject' ? 1 : 0);
    }
});

test('group keyboard moves use 8 or Shift-24 and clamp the whole group at zero', async () => {
    const h = harness(null, { grouped: true, inset: 64 });
    h.registration.onKeyDown(h.event(h.groupTitle, { key: 'ArrowRight' }));
    h.registration.onKeyDown(h.event(h.groupTitle, { key: 'ArrowRight', shiftKey: true }));
    assert.equal(h.parent.style.left, '132px'); assert.equal(h.child.style.left, '532px');
    assert.equal(h.calls.filter(call => call.method === 'OnTableGroupMoved').length, 0);
    h.registration.onKeyUp(h.event(h.groupTitle, { key: 'ArrowRight' })); await h.settle();
    assert.deepEqual(h.calls.filter(call => call.method === 'OnTableGroupMoved')[0].args, ['g1', 32, 0]);
    h.registration.onPointerDown(h.event(h.groupTitle));
    h.registration.onPointerMove(h.event(h.groupTitle, { clientX: -1000, clientY: -1000 })); h.flush();
    assert.equal(h.parent.style.left, '0px'); assert.equal(h.child.style.left, '400px');
    assert.equal(h.tableGroup.style.top, '-44px');
    assert.ok((parseFloat(h.tableGroup.style.top) + 64) >= 0);
});

test('group frames follow individual nodes, contribute to Fit, and only headers are routing obstacles', () => {
    const h = harness(null, { grouped: true, inset: 64 });
    const route = h.registration.routes.get('r1');
    assert.equal(route.obstacles.length, 3);
    const header = route.obstacles[2];
    assert.equal(header.bottom - header.top, 28);
    assert.equal(parseFloat(h.tableGroup.style.height), 300);
    h.child.style.left = '700px'; h.registration.updateEdges();
    assert.equal(parseFloat(h.tableGroup.style.width), 732);
    assert.ok(h.registration.modelBounds.maxX >= 816);
    const original = h.child.style.left;
    h.interop.fit('canvas');
    assert.equal(h.child.style.left, original);
});

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

test('segment dragging slides both corners without splitting the line and repeated moves reuse saved guides', async () => {
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
    assert.equal(preview.Waypoints.length, 2);
    assert.deepEqual(preview.Waypoints.map(point => [point.X, point.Y]),
        [first, second].map(point => [point.x + (horizontal ? 0 : 20), point.y + (horizontal ? 20 : 0)]));
    assert.deepEqual(copy(h.registration.routes.get('r1').points), [copy(route.points[0]),
        ...preview.Waypoints.map(point => ({ x: point.X, y: point.Y })), copy(route.points.at(-1))]);
    h.registration.onPointerUp(h.event(handle)); await h.settle();
    assert.equal(h.commits().length, 1);
    assert.equal(h.commits()[0].args[1].Waypoints[0].Id, preview.Waypoints[0].Id);
    const savedIds = preview.Waypoints.map(point => point.Id);
    const next = h.segment();
    h.registration.onPointerDown(h.event(next));
    h.registration.onPointerMove(h.event(next, { clientX: -12 })); h.flush();
    h.registration.onPointerUp(h.event(next)); await h.settle();
    const saved = h.commits().at(-1).args[1];
    assert.deepEqual(saved.Waypoints.map(point => point.Id), savedIds);
    assert.equal(h.registration.routes.get('r1').points.length, 4);
    const reopened = harness(saved);
    assert.deepEqual(copy(reopened.registration.routes.get('r1').points), copy(h.registration.routes.get('r1').points));
});

test('horizontal segment slides preserve unrelated guides and both adjacent vertical legs', async () => {
    const h = harness(manual([{ Id: 'before', X: 240, Y: 118 }, { Id: 'first', X: 240, Y: 60 },
        { Id: 'last', X: 460, Y: 60 }, { Id: 'after', X: 460, Y: 278 }]));
    const handle = h.handles.querySelectorAll('[data-route-handle="segment"]').find(handle => {
        const index = Number(handle.dataset.segmentIndex), points = h.registration.routes.get('r1').points;
        return points[index].y === 60 && points[index + 1].y === 60;
    });
    assert.ok(handle);
    h.registration.onPointerDown(h.event(handle));
    h.registration.onPointerMove(h.event(handle, { clientY: 10 })); h.flush();
    h.registration.onPointerUp(h.event(handle)); await h.settle();
    const guides = h.commits()[0].args[1].Waypoints;
    assert.deepEqual(guides, [{ Id: 'before', X: 240, Y: 118 }, { Id: 'first', X: 240, Y: 70 },
        { Id: 'last', X: 460, Y: 70 }, { Id: 'after', X: 460, Y: 278 }]);
    assert.ok(!h.registration.routes.get('r1').usedAutomaticFallback);
});

test('a straight run with a saved midpoint remains one sliding segment', async () => {
    const h = harness(manual([{ Id: 'top', X: 350, Y: 118 }, { Id: 'middle', X: 350, Y: 198 }, { Id: 'bottom', X: 350, Y: 278 }]));
    assert.equal(h.handles.querySelectorAll('[data-route-handle="segment"]').length, 1);
    const handle = h.segment();
    assert.notEqual(handle.getAttribute('transform'), h.handles.querySelectorAll('[data-route-handle="waypoint"]')[1].getAttribute('transform'));
    h.registration.onKeyDown(h.event(handle, { key: 'ArrowRight', shiftKey: true })); h.flush();
    assert.equal(h.commits().length, 0);
    h.registration.onKeyUp(h.event(handle, { key: 'ArrowRight' })); await h.settle();
    assert.deepEqual(h.commits()[0].args[1].Waypoints, [
        { Id: 'top', X: 374, Y: 118 }, { Id: 'middle', X: 374, Y: 198 }, { Id: 'bottom', X: 374, Y: 278 }]);
    assert.equal(h.handles.querySelectorAll('[data-route-handle="segment"]').length, 1);
});

for (const pointerType of ['mouse', 'touch', 'pen']) {
    test(`${pointerType} segment movement clamps to its lane, cancels cleanly, and returning to origin does not save`, async () => {
        const h = harness(null, { scale: 1.25 });
        const original = h.visible.getAttribute('d');
        const event = (handle, extra = {}) => h.event(handle, { pointerType, ...extra });
        let handle = h.segment();
        h.registration.onPointerDown(event(handle));
        h.registration.onPointerMove(event(handle, { clientX: -1000 })); h.flush();
        const points = h.registration.routes.get('r1').points;
        assert.equal(points.length, 4, 'Obstacle limits should not create detours');
        assert.ok(points[1].x > points[0].x && points[2].x < points[3].x);
        h.registration.onKeyDown(event(handle, { key: 'Escape' }));
        assert.equal(h.visible.getAttribute('d'), original); assert.equal(h.commits().length, 0);
        handle = h.segment();
        h.registration.onPointerDown(event(handle));
        h.registration.onPointerMove(event(handle, { clientX: 40 })); h.flush();
        h.registration.onPointerCancel(event(handle));
        assert.equal(h.visible.getAttribute('d'), original); assert.equal(h.commits().length, 0);
        handle = h.segment();
        h.registration.onPointerDown(event(handle));
        h.registration.onPointerMove(event(handle, { clientX: 40 })); h.flush();
        h.registration.onPointerMove(event(handle)); h.flush();
        h.registration.onPointerUp(event(handle)); await h.settle();
        assert.equal(h.visible.getAttribute('d'), original); assert.equal(h.commits().length, 0);
        assert.equal(h.canvas.captured.size, 0);
    });
}

test('straight endpoint-attached lines use explicit Add Bend rather than a sliding endpoint grip', async () => {
    const h = harness();
    h.child.style.top = h.parent.style.top; h.registration.updateEdges();
    assert.equal(h.segment(), null);
    await h.interop.addBend('canvas', 'r1');
    assert.equal(h.commits()[0].args[1].Waypoints.length, 1);
});

test('Add Bend inserts in route order even when an entire straight run contains existing guides', async () => {
    const h = harness(manual([{ Id: 'top', X: 350, Y: 118 }, { Id: 'early', X: 350, Y: 158 }, { Id: 'bottom', X: 350, Y: 278 }]));
    await h.interop.addBend('canvas', 'r1');
    const guides = h.commits()[0].args[1].Waypoints;
    assert.deepEqual(guides.map(point => point.Y), [118, 158, 198, 278]);
    assert.equal(guides[1].Id, 'early');
});

test('segment slides stop before an intervening table instead of routing a split around it', async () => {
    const h = harness(manual([{ Id: 'top', X: 350, Y: 118 }, { Id: 'bottom', X: 350, Y: 278 }]));
    const blocker = new h.parent.constructor('div', 'schema-node');
    blocker.dataset.table = 'blocker'; blocker.style.left = '420px'; blocker.style.top = '180px';
    blocker.fixedWidth = 40; blocker.fixedHeight = 40; h.stage.append(blocker); h.registration.updateEdges();
    const handle = h.segment();
    h.registration.onPointerDown(h.event(handle));
    h.registration.onPointerMove(h.event(handle, { clientX: 1000 })); h.flush();
    const points = h.registration.routes.get('r1').points;
    assert.equal(points.length, 4);
    assert.ok(points[1].x <= 402 && points[1].x === points[2].x);
    h.registration.onPointerUp(h.event(handle)); await h.settle();
    assert.equal(h.commits().length, 1);
});

test('rejected segment saves restore the route and keyboard movement along the segment is a no-op', async () => {
    const h = harness(null, { commit: async () => false });
    const path = h.visible.getAttribute('d');
    let handle = h.segment();
    h.registration.onKeyDown(h.event(handle, { key: 'ArrowUp' })); h.flush();
    h.registration.onKeyUp(h.event(handle, { key: 'ArrowUp' })); await h.settle();
    assert.equal(h.commits().length, 0);
    handle = h.segment();
    h.registration.onPointerDown(h.event(handle));
    h.registration.onPointerMove(h.event(handle, { clientX: 20 })); h.flush();
    h.registration.onPointerUp(h.event(handle)); await h.settle();
    assert.equal(h.commits().length, 1); assert.equal(h.visible.getAttribute('d'), path);
    assert.equal(h.group.dataset.connectorLayout, 'null');
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
