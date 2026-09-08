const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../../main/resources/review/review.js'), 'utf8');

// Execute the shipped script. Only its browser IO boundary is replaced; no
// production functions are copied or overridden. Initial rendering has no rows.
function reviewApp() {
  function element() {
    const attributes = new Map();
    const classes = new Set();
    return {
      value: '', hidden: false, textContent: '', focused: false,
      addEventListener() {}, appendChild() {}, replaceChildren() {},
      querySelectorAll: () => [],
      setAttribute: (key, value) => attributes.set(key, value),
      getAttribute: key => attributes.get(key),
      focus() { this.focused = true; },
      classList: { add: value => classes.add(value), remove: value => classes.delete(value) }
    };
  }
  const elements = new Map();
  const getElement = id => {
    if (!elements.has(id)) elements.set(id, element());
    return elements.get(id);
  };
  class FixedDate extends Date {
    constructor(...args) { super(...(args.length ? args : [2026, 8, 8, 12, 0, 0])); }
    static now() { return new Date(2026, 8, 8, 12, 0, 0).getTime(); }
  }
  const context = vm.createContext({
    REVIEW_DATA: {scan_path: '/data', findings: []}, Date: FixedDate,
    window: {},
    document: {
      getElementById: getElement,
      querySelector: () => getElement('tbody'),
      querySelectorAll: () => [],
      createDocumentFragment: element,
      createElement: element
    }
  });
  vm.runInContext(source, context, {filename: 'review.js'});
  return {
    elements, data: context.REVIEW_DATA,
    run: expression => vm.runInContext(expression, context),
    call(name, ...args) {
      context.testArgs = args;
      try { return vm.runInContext(`${name}(...testArgs)`, context); }
      finally { delete context.testArgs; }
    }
  };
}
const plain = value => JSON.parse(JSON.stringify(value));

test('CSV parser keeps BOM, quoted delimiters, escaped quotes and multiline fields correct', () => {
  const app = reviewApp();
  const csv = '\uFEFFkey,reason\r\na,"comma, quote ""here""\nnext line"\r\nb,plain\r\n';
  assert.deepEqual(plain(app.call('parseDelimitedText', csv, ',')), [
    ['key', 'reason'], ['a', 'comma, quote "here"\nnext line'], ['b', 'plain']
  ]);
});

test('TSV parser preserves empty cells and accepts an empty input', () => {
  const app = reviewApp();
  assert.deepEqual(plain(app.call('parseDelimitedText', 'a\t\tc\r\n\tlast\t', '\t')), [['a', '', 'c'], ['', 'last', '']]);
  assert.deepEqual(plain(app.call('parseDelimitedText', '', ',')), []);
});

test('CSV export escapes separators and neutralizes formula prefixes', () => {
  const app = reviewApp();
  for (const value of ['=SUM(A1)', '+1', '-2', '@name']) {
    assert.equal(app.call('escapeCsvCell', value), "'" + value);
  }
  assert.equal(app.call('escapeCsvCell', 'a,"b"\n한글'), '"a,""b""\n한글"');
  assert.equal(app.call('escapeCsvCell', null), '');
  assert.equal(app.call('escapeCsvCell', 0), '0');
});

test('imported decisions accept supported aliases and reject unknown values', () => {
  const app = reviewApp();
  for (const value of ['오탐', ' FP ', 'False Positive']) assert.equal(app.call('normalizeImportedDecision', value), 'false_positive');
  for (const value of ['정탐', ' TP ', 'TRUE_POSITIVE']) assert.equal(app.call('normalizeImportedDecision', value), 'true_positive');
  assert.equal(app.call('normalizeImportedDecision', '   '), '');
  assert.equal(app.call('normalizeImportedDecision', 'approved'), null);
});

test('false-positive export omits stale action fields and preserves zero counts', () => {
  const actual = reviewApp().call('sanitizeResponse', {
    finding_key: 'a', sample_row_count: 0, decision: 'false_positive',
    false_positive_reason: 'test data', action_plan: 'stale', action_due_date: '2026-09-10', expires_at: '2020-01-01'
  });
  assert.deepEqual(plain(actual), {
    finding_key: 'a', sample_row_count: 0, decision: 'false_positive',
    false_positive_reason: 'test data', expires_at: '9999-12-31'
  });
});

test('true-positive export omits stale allowlist fields', () => {
  const actual = reviewApp().call('sanitizeResponse', {
    finding_key: 'a', decision: 'true_positive', action_plan: 'mask', action_due_date: '2026-09-10',
    false_positive_reason: 'stale', expires_at: '9999-12-31', allowlist_scope: 'recurring'
  });
  assert.deepEqual(plain(actual), {finding_key: 'a', decision: 'true_positive', action_plan: 'mask', action_due_date: '2026-09-10'});
});

test('validation identifies missing and unsupported decisions per row', () => {
  const app = reviewApp();
  const errors = app.call('validateResponses', [{decision: ''}, {decision: 'approved'}]);
  assert.deepEqual(plain(errors.map(({index, field}) => ({index, field}))), [{index: 0, field: 'decision'}, {index: 1, field: 'decision'}]);
  assert.deepEqual(plain(app.call('validateResponses', [])), []);
});

test('false positives require a nonblank reason', () => {
  const app = reviewApp();
  assert.equal(app.call('validateResponses', [{decision: 'false_positive', false_positive_reason: ' \t '}])[0].field, 'false_positive_reason');
  assert.deepEqual(plain(app.call('validateResponses', [{decision: 'false_positive', false_positive_reason: 'known dummy'}])), []);
});

test('true positives require both action fields in their existing field order', () => {
  const errors = reviewApp().call('validateResponses', [{decision: 'true_positive', action_plan: '', action_due_date: ''}]);
  assert.deepEqual(plain(errors.map(error => error.field)), ['action_plan', 'action_due_date']);
});

test('action dates include today and day 30 and reject outside or impossible dates', () => {
  const app = reviewApp();
  for (const date of ['2026-09-08', '2026-10-08']) {
    assert.deepEqual(plain(app.call('validateResponses', [{decision: 'true_positive', action_plan: 'mask', action_due_date: date}])), []);
  }
  for (const date of ['2026-09-07', '2026-10-09', '2026-09-31', 'today']) {
    assert.equal(app.call('validateResponses', [{decision: 'true_positive', action_plan: 'mask', action_due_date: date}])[0].field, 'action_due_date');
  }
  assert.equal(app.call('isDateOnly', '2024-02-29'), true);
  assert.equal(app.call('isDateOnly', '2025-02-29'), false);
});

test('numeric sorting preserves ties by original index in both directions', () => {
  const app = reviewApp();
  app.data.findings = [{match_count: 10}, {match_count: 2}, {match_count: 2}];
  const rows = [2, 0, 1];
  app.run("sortState = {key: 'match_count', direction: 'asc'}");
  assert.deepEqual(plain(app.call('sortRows', rows)), [1, 2, 0]);
  app.run("sortState.direction = 'desc'");
  assert.deepEqual(plain(app.call('sortRows', rows)), [0, 1, 2]);
  assert.deepEqual(rows, [2, 0, 1]);
});

test('sorting editable values uses current form state after an edit', () => {
  const app = reviewApp();
  app.data.findings = [{}, {}];
  app.call('updateFormState', 0, 'action_plan', 'z');
  app.call('updateFormState', 1, 'action_plan', 'a');
  app.run("sortState = {key: 'action_plan', direction: 'asc'}");
  assert.deepEqual(plain(app.call('sortRows', [0, 1])), [1, 0]);
  app.call('updateFormState', 0, 'action_plan', '0');
  assert.deepEqual(plain(app.call('sortRows', [0, 1])), [0, 1]);
});

test('first validation error follows displayed row order and keeps the first field', () => {
  const app = reviewApp();
  const errors = [{index: 0, field: 'decision'}, {index: 2, field: 'action_plan'}, {index: 2, field: 'action_due_date'}];
  app.elements.get('tbody').querySelectorAll = () => [2, 1, 0].map(index => ({getAttribute: () => String(index)}));
  assert.equal(app.call('firstValidationErrorInDisplayOrder', errors), errors[1]);
  app.elements.get('tbody').querySelectorAll = () => [];
  assert.equal(app.call('firstValidationErrorInDisplayOrder', errors), errors[0]);
  assert.equal(app.call('firstValidationErrorInDisplayOrder', []), undefined);
});

test('invalid responders get an input error and valid responders clear it', () => {
  const app = reviewApp();
  const input = app.elements.get('responder');
  for (const value of ['', 'Owner1', 'owner@example.com']) {
    input.value = value;
    assert.equal(app.call('validateResponder'), false);
    assert.equal(input.getAttribute('aria-invalid'), 'true');
    assert.equal(input.focused, true);
  }
  input.value = 'owner1';
  assert.equal(app.call('validateResponder'), true);
  assert.equal(input.getAttribute('aria-invalid'), 'false');
});

test('form snapshots cannot mutate stored input and unknown fields are ignored', () => {
  const app = reviewApp();
  app.call('updateFormState', 0, 'decision', 'true_positive');
  app.call('updateFormState', 0, 'unexpected', 'value');
  const snapshot = app.call('collectFormValues');
  snapshot[0].decision = 'false_positive';
  assert.equal(app.call('collectFormValues')[0].decision, 'true_positive');
  assert.equal(Object.hasOwn(app.call('collectFormValues')[0], 'unexpected'), false);
});

test('sample rendering escapes HTML from detected values', () => {
  const html = reviewApp().call('renderSampleCell', {evidence_samples: [{sample_matched_fragment: '<script>', sample_raw_value: 'a&"b'}]});
  assert.equal(html, '&lt;script&gt;\na&amp;&quot;b');
});

test('detection percentages handle zero, missing and nonnumeric denominators', () => {
  const app = reviewApp();
  assert.equal(app.call('formatDetectionPercent', {match_count: 1, sampled_row_count: 3}), '33.33');
  assert.equal(app.call('formatDetectionPercent', {match_count: 0, sampled_row_count: 3}), '0.00');
  for (const value of [0, -1, undefined, 'unknown']) {
    assert.equal(app.call('formatDetectionPercent', {match_count: 1, sampled_row_count: value}), '');
  }
});

test('download filenames replace path separators and handle blank scan paths', () => {
  const app = reviewApp();
  assert.equal(app.call('formatResponseScanPath', 'hdfs://nn/data/a b'), 'hdfs-nn-data-a-b');
  assert.equal(app.call('formatResponseScanPath', ' /// '), 'scan');
  assert.equal(app.call('formatResponseScanPath', null), 'scan');
});
