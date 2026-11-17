import test from 'node:test';
import assert from 'node:assert';
import { SchemaType, DataFlowType } from '@tak-ps/etl';

// task.ts calls Task.init() at module scope which requires an ETL environment,
// so these must be set before the dynamic import below
process.env.ETL_API = process.env.ETL_API || 'http://localhost:5001';
process.env.ETL_LAYER = process.env.ETL_LAYER || '1';
process.env.ETL_TOKEN = process.env.ETL_TOKEN || 'etl.test-token';

const { default: Task } = await import('../task.js');

test('Task static config', () => {
    assert.equal(Task.name, 'etl-capnz');
    assert.deepEqual(Task.flow, [DataFlowType.Incoming]);
});

test('Incoming Input schema', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Input, DataFlowType.Incoming);

    assert.equal(schema.type, 'object');
    for (const key of [
        'RSS_URL',
        'Headers',
        'Timeout',
        'Retries'
    ]) {
        assert.ok(schema.properties[key], `Env schema missing property: ${key}`);
    }

    assert.equal(schema.properties.Timeout.type, 'number');
    assert.equal(schema.properties.Timeout.default, 30000);
    assert.equal(schema.properties.Retries.type, 'number');
    assert.equal(schema.properties.Retries.default, 2);
});

test('Incoming Output schema', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Output, DataFlowType.Incoming);

    // No explicit output schema is declared - features are built dynamically
    // from the parsed CAP alert, so this is intentionally an empty object.
    assert.equal(schema.type, 'object');
    assert.deepEqual(schema.properties, {});
});

test('Outgoing flow is not provided', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Input, DataFlowType.Outgoing);

    assert.deepEqual(schema.properties, {});
});

const { HIDDEN_MARKER_PROPERTIES, NO_EXPIRY_STALE_MS, alertStale } = await import('../task.js');
const { CoTParser } = await import('@tak-ps/node-cot');

test('alertStale uses expires when present', () => {
    assert.equal(alertStale('2030-01-02T00:00:00+13:00', '2030-01-01T00:00:00Z'), '2030-01-01T11:00:00.000Z');
});

test('alertStale falls back to sent + 24h without expires', () => {
    assert.equal(NO_EXPIRY_STALE_MS, 24 * 60 * 60 * 1000);
    assert.equal(alertStale(undefined, '2030-01-01T00:00:00Z'), '2030-01-02T00:00:00.000Z');
});

test('Polygon marker converts to an invisible, expiring marker with a polyline', async () => {
    const stale = alertStale(undefined, '2030-01-01T00:00:00Z');
    const cot = await CoTParser.from_geojson({
        id: 'alert-1',
        type: 'Feature',
        properties: {
            callsign: 'CAP Alert',
            type: 'a-f-X-i',
            stale,
            ...HIDDEN_MARKER_PROPERTIES,
            stroke: '#ff0000',
            'stroke-opacity': 0.5,
            fill: '#ff0000',
            'fill-opacity': 0.4
        },
        geometry: { type: 'Polygon', coordinates: [[[170, -43], [171, -43], [171, -44], [170, -44], [170, -43]]] }
    } as never);
    const event = cot.raw.event;

    // A u-d-f drawing would never expire in ATAK
    assert.equal(event._attributes.type, 'a-f-X-i');
    assert.equal(event._attributes.stale, stale);
    assert.ok(event.detail.shape?.polyline, 'detail.shape.polyline is set');

    // A usericon makes ATAK delete the polygon; alpha 0 hides the "?" marker
    assert.equal(event.detail.usericon, undefined);
    const argb = Number(event.detail.color?._attributes?.argb);
    assert.ok(Number.isFinite(argb));
    assert.equal((argb >>> 24) & 0xff, 0);
});
