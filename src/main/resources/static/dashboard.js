function formatTimestamp(ts) {
    if (!ts) return '—';

    const d = new Date(ts);

    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');

    const hh = String(d.getUTCHours()).padStart(2, '0');
    const min = String(d.getUTCMinutes()).padStart(2, '0');
    const ss = String(d.getUTCSeconds()).padStart(2, '0');

    return `${yyyy}-${mm}-${dd} ${hh}:${min}:${ss}`;
}

async function loadMetrics() {
    try {
        const response = await fetch('/metrics');
        const data = await response.json();

        /* -------- Counters -------- */
        document.getElementById('clicksReceived').textContent =
            data.clicksReceived ?? 0;

        document.getElementById('pageViewsReceived').textContent =
            data.pageViewsReceived ?? 0;

        document.getElementById('pageViewsEmitted').textContent =
            data.pageViewsEmitted ?? 0;

        document.getElementById('pageViewsUpdated').textContent =
            data.pageViewsUpdated ?? 0;

        /* -------- State sizes -------- */
        document.getElementById('clickStateSize').textContent =
            data.clickStateSize ?? 0;

        document.getElementById('pageViewStateSize').textContent =
            data.pageViewStateSize ?? 0;

        /* -------- Last updated -------- */
        document.getElementById('lastUpdatedAt').textContent =
            data.lastUpdatedAt
                ? new Date(data.lastUpdatedAt).toISOString()
                : '—';

        /* -------- Join Watermarks -------- */
        const table = document.getElementById('joinWatermarkTable');
        table.innerHTML = '';

        (data.joinWatermarks || []).forEach(wm => {
            const row = document.createElement('tr');

            const cell = value => {
                const td = document.createElement('td');
                td.textContent = value ?? '—';
                return td;
            };

            row.appendChild(cell(wm.partition));

            row.appendChild(cell(
                wm.pageViewsMaxEventTime
                    ? formatTimestamp(wm.pageViewsMaxEventTime)
                    : '—'
            ));

            row.appendChild(cell(
                wm.adClicksMaxEventTime
                    ? formatTimestamp(wm.adClicksMaxEventTime)
                    : '—'
            ));

            row.appendChild(cell(
                wm.joinWatermark
                    ? formatTimestamp(wm.joinWatermark)
                    : '—'
            ));

            table.appendChild(row);
        });

    } catch (err) {
        console.error('Failed to load metrics', err);
    }
}

/* Initial load */
loadMetrics();

/* Refresh every second */
setInterval(loadMetrics, 1000);
