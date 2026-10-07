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
