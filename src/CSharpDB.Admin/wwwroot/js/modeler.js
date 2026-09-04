// Read-only exports: use rendered geometry, never ask Blazor to arrange or persist anything.
window.modelerFiles = {
    downloadText: (name, type, text) => window.modelerFiles.downloadBlob(name, new Blob([text], { type })),
    downloadBlob: (name, blob) => {
        const url = URL.createObjectURL(blob), link = document.createElement('a');
        link.href = url; link.download = name; document.body.appendChild(link);
        link.click(); link.remove();
        // Allow the browser download manager to consume the URL before releasing it.
        setTimeout(() => URL.revokeObjectURL(url), 10000);
    },
    exportCanvas: async (format, canvasId) => {
        const canvas = document.getElementById(canvasId);
        if (!canvas) throw new Error('The model canvas is not available.');
        if (!['svg', 'png'].includes(format)) throw new Error('Unsupported export format.');
        const stage = canvas.querySelector('.schema-canvas-stage');
        const ns = 'http://www.w3.org/2000/svg';
        const make = (tag, attrs, text) => {
            const element = document.createElementNS(ns, tag);
            for (const [key, value] of Object.entries(attrs || {})) element.setAttribute(key, String(value));
            if (text !== undefined) element.textContent = text;
            return element;
        };
        const width = Math.ceil(parseFloat(stage.style.width)), height = Math.ceil(parseFloat(stage.style.height));
        const inset = Number(canvas.dataset.canvasInset || 64);
        const svg = make('svg', { xmlns: ns, width, height, viewBox: `0 0 ${width} ${height}` });
        const canvasStyle = getComputedStyle(canvas);
        svg.append(make('rect', { width, height, fill: canvasStyle.backgroundColor === 'rgba(0, 0, 0, 0)' ? '#171821' : canvasStyle.backgroundColor }));
        const scene = make('g', { transform: `translate(${inset},${inset})`, 'font-family': 'Arial, sans-serif' });
        svg.append(scene);
        for (const group of stage.querySelectorAll('.schema-table-group')) {
            const style = getComputedStyle(group), x = parseFloat(group.style.left), y = parseFloat(group.style.top);
            scene.append(make('rect', { x, y, width: group.offsetWidth, height: group.offsetHeight, rx: 6, fill: style.backgroundColor, stroke: style.borderColor }));
            scene.append(make('text', { x: x + 10, y: y + 19, fill: getComputedStyle(group.querySelector('button')).color, 'font-size': 13 }, group.querySelector('button').textContent));
        }
        const original = stage.querySelector('svg');
        const edges = original.cloneNode(true);
        const sources = [original, ...original.querySelectorAll('*')], targets = [edges, ...edges.querySelectorAll('*')];
        for (let i = 0; i < sources.length; i++) {
            const style = getComputedStyle(sources[i]);
            for (const property of ['fill', 'stroke', 'stroke-width', 'stroke-dasharray', 'font-size', 'font-family', 'marker-start', 'marker-end']) {
                let value = style.getPropertyValue(property);
                value = value.replace(/url\(["']?[^#)]*#([^"')]+)["']?\)/g, 'url(#$1)');
                if (value) targets[i].style.setProperty(property, value);
            }
            targets[i].style.opacity = '1';
        }
        // Transient hover labels use foreignObject in the live canvas. Exclude them and
        // interaction grips so the standalone SVG/PNG contains only portable SVG geometry.
        edges.querySelectorAll('[data-model-edge="hit"], [data-model-route-handles], [data-model-label]').forEach(element => element.remove());
        scene.append(edges);
        for (const node of stage.querySelectorAll('.schema-node')) {
            const style = getComputedStyle(node), x = parseFloat(node.style.left), y = parseFloat(node.style.top);
            scene.append(make('rect', { x, y, width: node.offsetWidth, height: node.offsetHeight, rx: 5, fill: style.backgroundColor, stroke: style.borderColor }));
            const header = node.querySelector('.designer-table-node-header');
            scene.append(make('rect', { x, y, width: node.offsetWidth, height: header.offsetHeight, fill: getComputedStyle(header).backgroundColor }));
            scene.append(make('text', { x: x + 9, y: y + 23, fill: getComputedStyle(header).color, 'font-size': 13, 'font-weight': 'bold' }, node.dataset.table));
            const meta = node.querySelector('.schema-node-meta');
            let rowY = y + header.offsetHeight;
            if (meta) { scene.append(make('text', { x: x + 8, y: rowY + 16, fill: getComputedStyle(meta).color, 'font-size': 10 }, meta.textContent.replace(/\s+/g, ' ').trim())); rowY += meta.offsetHeight; }
            for (const row of node.querySelectorAll('.schema-node-col')) {
                const type = row.querySelector('.designer-col-type').textContent;
                const name = row.querySelector('.designer-col-name');
                const flags = [...row.querySelectorAll('[title]')].map(element => element.title).join(', ');
                const text = make('text', { x: x + 8, y: rowY + row.offsetHeight / 2 + 4, fill: getComputedStyle(name).color, 'font-size': 11, 'font-weight': name.classList.contains('pk') ? 'bold' : 'normal' }, `${type}  ${name.textContent}`);
                text.append(make('title', {}, flags)); scene.append(text); rowY += row.offsetHeight;
            }
        }
        const text = new XMLSerializer().serializeToString(svg);
        if (format === 'svg') { window.modelerFiles.downloadText('data-model.svg', 'image/svg+xml;charset=utf-8', text); return; }
        const image = new Image();
        // A self-contained SVG data URL avoids cross-origin resources and foreignObject tainting.
        image.src = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(text);
        await image.decode();
        const scale = Math.min(1, 8192 / Math.max(width, height), Math.sqrt(16000000 / (width * height)));
        const output = document.createElement('canvas'); output.width = Math.ceil(width * scale); output.height = Math.ceil(height * scale);
        output.getContext('2d').drawImage(image, 0, 0, output.width, output.height);
        const blob = await new Promise(resolve => output.toBlob(resolve, 'image/png'));
        if (!blob) throw new Error('PNG encoding failed. Try SVG export.');
        window.modelerFiles.downloadBlob('data-model.png', blob);
    }
};
