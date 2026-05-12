    document.getElementById('scanPath').textContent = REVIEW_DATA.scan_path;
    const tbody = document.querySelector('#findingsTable tbody');
    const escapeHtml = value => String(value ?? '').replace(/[&<>"']/g, ch => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;'
    }[ch]));
    const PiiTypeLabels = {
      phone_number: '전화번호',
      email: '이메일',
      resident_registration_number: '주민등록번호',
      foreign_registration_number: '외국인등록번호',
      driver_license_number: '운전면허번호',
      address: '주소',
      bank_account_number: '계좌번호',
      credit_card_number: '신용카드번호',
      passport_number: '여권번호',
      ip_address: 'IP 주소'
    };
    function displayPiiType(value) {
      return PiiTypeLabels[value] || value;
    }
    function formatResponseTimestamp(date) {
      const pad = value => String(value).padStart(2, '0');
      return String(date.getFullYear()) +
        pad(date.getMonth() + 1) +
        pad(date.getDate()) +
        '-' +
        pad(date.getHours()) +
        pad(date.getMinutes()) +
        pad(date.getSeconds());
    }
    function formatResponseScanPath(scanPath) {
      const safePath = String(scanPath || 'scan')
        .trim()
        .replace(/[\\/:*?"<>|]+/g, '-')
        .replace(/\s+/g, '-')
        .replace(/-+/g, '-')
        .replace(/^-+|-+$/g, '');
      return safePath || 'scan';
    }
    const FormFieldDefaults = {
      decision: '',
      false_positive_reason: '',
      action_plan: '',
      action_due_date: ''
    };
    const FormFieldNames = Object.keys(FormFieldDefaults);
    const formState = new Map();
    const hydratedRows = new Map();
    const validationState = new Map();
    const collator = new Intl.Collator('ko-KR', { numeric: true, sensitivity: 'base' });
    const ActionDueDateWindowDays = 30;
    const PermanentFalsePositiveExpiresAt = '9999-12-31';
    const ResponderPattern = /^[a-z0-9]+$/;
    const ReviewCsvHeaders = [
      'finding_key',
      '경로',
      'Hive 테이블',
      '컬럼명',
      '개인정보 유형',
      '샘플 행 수',
      '검출 건수',
      '검출비율(%)',
      '검출샘플(검출값/데이터)',
      '판정',
      '오탐 사유',
      '정탐 조치 계획',
      '조치 예정일'
    ];
    const ReviewCsvEditableHeaders = {
      decision: '판정',
      false_positive_reason: '오탐 사유',
      action_plan: '정탐 조치 계획',
      action_due_date: '조치 예정일'
    };
    const SupportedDecisions = new Set(['false_positive', 'true_positive']);
    const findingIndexByKey = new Map(REVIEW_DATA.findings.map((finding, index) => [finding.finding_key, index]));
    function dateOnlyFromLocal(date) {
      const pad = value => String(value).padStart(2, '0');
      return String(date.getFullYear()) + '-' + pad(date.getMonth() + 1) + '-' + pad(date.getDate());
    }
    function addLocalDays(date, days) {
      const next = new Date(date.getFullYear(), date.getMonth(), date.getDate());
      next.setDate(next.getDate() + days);
      return next;
    }
    function todayDateOnly() {
      return dateOnlyFromLocal(new Date());
    }
    function maxActionDueDate() {
      return dateOnlyFromLocal(addLocalDays(new Date(), ActionDueDateWindowDays));
    }
    function isDateOnly(value) {
      if (!/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(value)) {
        return false;
      }
      const parts = value.split('-').map(part => Number(part));
      const date = new Date(parts[0], parts[1] - 1, parts[2]);
      return date.getFullYear() === parts[0] &&
        date.getMonth() === parts[1] - 1 &&
        date.getDate() === parts[2];
    }
    function isActionDueDateWithinWindow(value) {
      if (!isDateOnly(value)) {
        return false;
      }
      return value >= todayDateOnly() && value <= maxActionDueDate();
    }
    function applyActionDueDateLimits(input) {
      if (!input) {
        return;
      }
      input.min = todayDateOnly();
      input.max = maxActionDueDate();
    }
    applyActionDueDateLimits(document.getElementById('bulkTruePositiveDueDate'));
    function defaultFormState() {
      return Object.assign({}, FormFieldDefaults);
    }
    REVIEW_DATA.findings.forEach((finding, index) => {
      formState.set(index, defaultFormState());
    });
    let sortState = { key: null, direction: 'asc' };
    let rowObserver = null;
    function getFormState(index) {
      const numericIndex = Number(index);
      if (!formState.has(numericIndex)) {
        formState.set(numericIndex, defaultFormState());
      }
      return formState.get(numericIndex);
    }
    function updateFormState(index, field, value) {
      if (!FormFieldNames.includes(field)) {
        return;
      }
      getFormState(index)[field] = value;
    }
    function isBlank(value) {
      return String(value ?? '').trim() === '';
    }
    function formValuesSnapshot() {
      const values = {};
      formState.forEach((state, index) => {
        values[index] = Object.assign({}, state);
      });
      return values;
    }
    function collectFormValues() {
      return formValuesSnapshot();
    }
    function sampleSortText(finding) {
      return finding.evidence_samples.map(sample => [
        sample.sample_matched_fragment,
        sample.sample_raw_value
      ].join(' ')).join(' ');
    }
    function detectionPercentValue(finding) {
      const matchCount = Number(finding.match_count);
      const sampledRowCount = Number(finding.sampled_row_count);
      if (!Number.isFinite(matchCount) || !Number.isFinite(sampledRowCount) || sampledRowCount <= 0) {
        return null;
      }
      return matchCount / sampledRowCount * 100;
    }
    function formatDetectionPercent(finding) {
      const percent = detectionPercentValue(finding);
      return percent === null ? '' : percent.toFixed(2);
    }
    function formSortText(index, fields) {
      const rowValues = getFormState(index);
      return fields.map(field => rowValues[field] || '').join(' ');
    }
    function existingActionSortText(finding) {
      const state = finding.action_plan_state || {};
      return [
        state.status_label,
        state.action_due_date,
        state.action_plan,
        state.responder
      ].filter(Boolean).join(' ');
    }
    function getSortValue(index) {
      const finding = REVIEW_DATA.findings[index];
      switch (sortState.key) {
        case 'path':
          return finding.file_identifier;
        case 'hive':
          return finding.hive_table_fqn;
        case 'column':
          return finding.column_name;
        case 'pii':
          return displayPiiType(finding.pii_type);
        case 'sampled_row_count':
          return Number(finding.sampled_row_count) || 0;
        case 'match_count':
          return Number(finding.match_count) || 0;
        case 'non_empty_match_ratio':
          return detectionPercentValue(finding) ?? 0;
        case 'sample':
          return sampleSortText(finding);
        case 'decision':
          return formSortText(index, ['decision']);
        case 'existing_action_status':
          return existingActionSortText(finding);
        case 'false_positive_reason':
        case 'action_plan':
        case 'action_due_date':
          return formSortText(index, [sortState.key]);
        default:
          return index;
      }
    }
    function compareSortValues(left, right) {
      if (Array.isArray(left) || Array.isArray(right)) {
        const leftArray = Array.isArray(left) ? left : [left];
        const rightArray = Array.isArray(right) ? right : [right];
        const length = Math.max(leftArray.length, rightArray.length);
        for (let i = 0; i < length; i += 1) {
          const result = compareSortValues(leftArray[i] ?? '', rightArray[i] ?? '');
          if (result !== 0) {
            return result;
          }
        }
        return 0;
      }
      if (typeof left === 'number' && typeof right === 'number') {
        return left - right;
      }
      return collator.compare(String(left ?? ''), String(right ?? ''));
    }
    function sortRows(rows) {
      if (!sortState.key) {
        return rows.slice();
      }
      const direction = sortState.direction === 'desc' ? -1 : 1;
      const sortKeys = new Map(rows.map(index => [index, getSortValue(index)]));
      return rows.slice().sort((left, right) => {
        const result = compareSortValues(sortKeys.get(left), sortKeys.get(right));
        return result === 0 ? left - right : result * direction;
      });
    }
    function setFieldValues(row, index) {
      Object.entries(getFormState(index)).forEach(([field, value]) => {
        const input = row.querySelector('[data-field="' + field + '"]');
        if (input) {
          input.value = value;
        }
      });
      updateDecisionButtons(row, index);
    }
    function updateDecisionButtons(row, index) {
      const decision = getFormState(index).decision;
      row.querySelectorAll('[data-decision-button]').forEach(button => {
        button.setAttribute(
          'aria-pressed',
          String(button.getAttribute('data-decision-button') === decision)
        );
      });
    }
    function applyDecisionVisibility(row) {
      if (!row) {
        return;
      }
      const decision = getFormState(Number(row.getAttribute('data-index'))).decision;
      row.querySelectorAll('[data-decision-section]').forEach(section => {
        section.hidden = section.getAttribute('data-decision-section') !== decision;
      });
    }
    function applyScopeVisibility(row) {
      return row;
    }
    function validationTarget(row, field) {
      return field === 'decision'
        ? row.querySelector('[data-validation-field="decision"]')
        : row.querySelector('[data-field="' + field + '"]');
    }
    function validationFocusTarget(row, field) {
      if (field === 'decision') {
        return row.querySelector('[data-decision-button]:not(:disabled)') || row.querySelector('[data-decision-button]');
      }
      return row.querySelector('[data-field="' + field + '"]');
    }
    function applyValidationState(row, index) {
      if (!row) {
        return;
      }
      row.querySelectorAll('.invalid-cell').forEach(cell => cell.classList.remove('invalid-cell'));
      row.querySelectorAll('[aria-invalid="true"]').forEach(field => field.setAttribute('aria-invalid', 'false'));
      const fields = validationState.get(Number(index));
      if (!fields) {
        return;
      }
      fields.forEach(field => {
        const target = validationTarget(row, field);
        if (!target) {
          return;
        }
        target.setAttribute('aria-invalid', 'true');
        const cell = target.closest('td') || target;
        cell.classList.add('invalid-cell');
      });
    }
    function setValidationState(errors) {
      validationState.clear();
      errors.forEach(error => {
        const fields = validationState.get(error.index) || new Set();
        fields.add(error.field);
        validationState.set(error.index, fields);
      });
      hydratedRows.forEach((row, index) => applyValidationState(row, index));
    }
    function clearValidationField(index, field) {
      const numericIndex = Number(index);
      const fields = validationState.get(numericIndex);
      if (!fields) {
        return;
      }
      fields.delete(field);
      if (fields.size === 0) {
        validationState.delete(numericIndex);
      }
      applyValidationState(hydratedRows.get(numericIndex), numericIndex);
    }
    const responderInput = document.getElementById('responder');
    const responderField = document.getElementById('responderField');
    const responderError = document.getElementById('responderError');
    function clearResponderValidation() {
      responderField.classList.remove('invalid-field');
      responderInput.setAttribute('aria-invalid', 'false');
      responderError.hidden = true;
    }
    function validateResponder() {
      if (ResponderPattern.test(responderInput.value.trim())) {
        clearResponderValidation();
        return true;
      }
      responderField.classList.add('invalid-field');
      responderInput.setAttribute('aria-invalid', 'true');
      responderError.hidden = false;
      responderInput.focus();
      return false;
    }
    function validateResponses(responses) {
      const errors = [];
      responses.forEach((response, index) => {
        if (!response.decision) {
          errors.push({ index, field: 'decision', message: '판정을 선택하세요.' });
        } else if (!SupportedDecisions.has(response.decision)) {
          errors.push({ index, field: 'decision', message: '지원하지 않는 판정입니다.' });
        } else if (response.decision === 'false_positive') {
          if (isBlank(response.false_positive_reason)) {
            errors.push({ index, field: 'false_positive_reason', message: '오탐 사유를 입력하세요.' });
          }
        } else if (response.decision === 'true_positive') {
          if (isBlank(response.action_plan)) {
            errors.push({ index, field: 'action_plan', message: '정탐 조치 계획을 입력하세요.' });
          }
          if (isBlank(response.action_due_date)) {
            errors.push({ index, field: 'action_due_date', message: '조치 예정일을 입력하세요.' });
          } else if (!isActionDueDateWithinWindow(response.action_due_date)) {
            errors.push({ index, field: 'action_due_date', message: '조치 예정일은 오늘부터 30일 이내여야 합니다.' });
          }
        }
      });
      return errors;
    }
    function focusFirstValidationError(error) {
      if (!error) {
        return;
      }
      const row = tbody.querySelector('tr[data-index="' + error.index + '"]');
      if (!row) {
        return;
      }
      hydrateRow(row);
      applyValidationState(row, error.index);
      const target = validationFocusTarget(row, error.field);
      const scrollTarget = validationTarget(row, error.field) || target || row;
      scrollTarget.scrollIntoView({ block: 'center', inline: 'center' });
      if (target && typeof target.focus === 'function') {
        target.focus();
      }
    }
    function firstValidationErrorInDisplayOrder(errors) {
      const rows = Array.from(tbody.querySelectorAll('tr[data-index]'));
      for (const row of rows) {
        const index = Number(row.getAttribute('data-index'));
        const error = errors.find(candidate => candidate.index === index);
        if (error) {
          return error;
        }
      }
      return errors[0];
    }
    function applyBulkTruePositivePlan() {
      const actionPlan = document.getElementById('bulkTruePositivePlan').value;
      const dueDate = document.getElementById('bulkTruePositiveDueDate').value;
      if (!actionPlan && !dueDate) {
        return;
      }
      const bulkSortKeys = new Set(['action_plan', 'action_due_date']);
      const shouldRefreshSort = bulkSortKeys.has(sortState.key);
      let changed = false;
      formState.forEach((values, index) => {
        if (values.decision === 'true_positive') {
          if (actionPlan) {
            values.action_plan = actionPlan;
            clearValidationField(index, 'action_plan');
          }
          if (dueDate) {
            values.action_due_date = dueDate;
            clearValidationField(index, 'action_due_date');
          }
          changed = true;
          if (!shouldRefreshSort) {
            updateHydratedRow(index);
          }
        }
      });
      if (changed && shouldRefreshSort) {
        renderFindings();
      }
    }
    function applyBulkFalsePositiveReason() {
      const reason = document.getElementById('bulkFalsePositiveReason').value;
      if (!reason) {
        return;
      }
      const bulkSortKeys = new Set(['false_positive_reason']);
      const shouldRefreshSort = bulkSortKeys.has(sortState.key);
      let changed = false;
      formState.forEach((values, index) => {
        if (values.decision === 'false_positive') {
          values.false_positive_reason = reason;
          clearValidationField(index, 'false_positive_reason');
          changed = true;
          if (!shouldRefreshSort) {
            updateHydratedRow(index);
          }
        }
      });
      if (changed && shouldRefreshSort) {
        renderFindings();
      }
    }
    function compactResponseFields(response) {
      return Object.fromEntries(Object.entries(response).filter(([, value]) =>
        value !== null && value !== undefined && value !== ''
      ));
    }
    function responseBase(response) {
      return {
        finding_key: response.finding_key,
        finding_hash: response.finding_hash,
        file_identifier: response.file_identifier,
        file_identifier_pattern: response.file_identifier_pattern,
        hive_database: response.hive_database,
        hive_table: response.hive_table,
        hive_table_fqn: response.hive_table_fqn,
        column_name: response.column_name,
        pii_type: response.pii_type,
        sample_row_count: response.sample_row_count,
        match_count: response.match_count,
        non_empty_match_ratio: response.non_empty_match_ratio,
        decision: response.decision
      };
    }
    function sanitizeResponse(response) {
      if (response.decision === 'false_positive') {
        return compactResponseFields(Object.assign(responseBase(response), {
          false_positive_reason: response.false_positive_reason,
          expires_at: PermanentFalsePositiveExpiresAt
        }));
      }
      if (response.decision === 'true_positive') {
        return compactResponseFields(Object.assign(responseBase(response), {
          action_plan: response.action_plan,
          action_due_date: response.action_due_date
        }));
      }
      return compactResponseFields(response);
    }
    function updateSortHeaders() {
      document.querySelectorAll('#findingsTable th[data-sort-key]').forEach(th => {
        const isActive = th.getAttribute('data-sort-key') === sortState.key;
        th.setAttribute('aria-sort', isActive ? (sortState.direction === 'asc' ? 'ascending' : 'descending') : 'none');
        const indicator = th.querySelector('.sort-indicator');
        if (indicator) {
          indicator.textContent = isActive ? (sortState.direction === 'asc' ? '▲' : '▼') : '';
        }
      });
    }
    function renderPlaceholderRow(index) {
      const finding = REVIEW_DATA.findings[index];
      const summary = [
        finding.file_identifier,
        finding.hive_table_fqn,
        finding.column_name,
        displayPiiType(finding.pii_type)
      ].filter(Boolean).join(' / ');
      return `<td colspan="13" class="placeholder-cell"><span hidden data-finding-key="${escapeHtml(finding.finding_key)}">${escapeHtml(finding.finding_key)}</span><span class="placeholder-summary">${escapeHtml(summary)}</span></td>`;
    }
    function renderSampleCell(finding) {
      const samples = finding.evidence_samples.map(sample =>
        escapeHtml(sample.sample_matched_fragment) + '\n' +
        escapeHtml(sample.sample_raw_value)
      ).join('\n---\n');
      return samples;
    }
    function sampleText(finding) {
      return finding.evidence_samples.map(sample =>
        String(sample.sample_matched_fragment ?? '') + '\n' +
        String(sample.sample_raw_value ?? '')
      ).join('\n---\n');
    }
    function neutralizeCsvFormulaValue(value) {
      const text = String(value ?? '');
      return /^[=+\-@]/.test(text) ? "'" + text : text;
    }
    function escapeCsvCell(value) {
      const text = neutralizeCsvFormulaValue(value);
      if (/[,\t\r\n"]/.test(text)) {
        return '"' + text.replace(/"/g, '""') + '"';
      }
      return text;
    }
    function decisionDisplayValue(value) {
      if (value === 'false_positive') {
        return '오탐';
      }
      if (value === 'true_positive') {
        return '정탐';
      }
      return '';
    }
    function reviewCsvRows() {
      const rows = [ReviewCsvHeaders];
      REVIEW_DATA.findings.forEach((finding, index) => {
        const values = getFormState(index);
        rows.push([
          finding.finding_key,
          finding.file_identifier,
          finding.hive_table_fqn,
          finding.column_name,
          displayPiiType(finding.pii_type),
          finding.sampled_row_count,
          finding.match_count,
          formatDetectionPercent(finding),
          sampleText(finding),
          decisionDisplayValue(values.decision),
          values.false_positive_reason,
          values.action_plan,
          values.action_due_date
        ]);
      });
      return rows;
    }
    function downloadReviewCsv() {
      const csv = reviewCsvRows()
        .map(row => row.map(escapeCsvCell).join(','))
        .join('\r\n');
      const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `review-${formatResponseScanPath(REVIEW_DATA.scan_path)}-${formatResponseTimestamp(new Date())}.csv`;
      link.click();
      URL.revokeObjectURL(url);
    }
    function parseDelimitedText(text, delimiter) {
      const rows = [];
      let row = [];
      let cell = '';
      let inQuotes = false;
      for (let index = 0; index < text.length; index += 1) {
        const ch = text[index];
        if (inQuotes) {
          if (ch === '"') {
            if (text[index + 1] === '"') {
              cell += '"';
              index += 1;
            } else {
              inQuotes = false;
            }
          } else {
            cell += ch;
          }
        } else if (ch === '"') {
          inQuotes = true;
        } else if (ch === delimiter) {
          row.push(cell);
          cell = '';
        } else if (ch === '\n') {
          row.push(cell);
          rows.push(row);
          row = [];
          cell = '';
        } else if (ch === '\r') {
          if (text[index + 1] === '\n') {
            index += 1;
          }
          row.push(cell);
          rows.push(row);
          row = [];
          cell = '';
        } else {
          cell += ch;
        }
      }
      if (cell !== '' || row.length > 0) {
        row.push(cell);
        rows.push(row);
      }
      if (rows.length > 0 && rows[0].length > 0) {
        rows[0][0] = rows[0][0].replace(/^\uFEFF/, '');
      }
      return rows;
    }
    function normalizeImportedDecision(value) {
      const text = String(value ?? '').trim();
      const normalized = text.toLowerCase().replace(/\s+/g, '_');
      if (!normalized) {
        return '';
      }
      if (normalized === '오탐' || normalized === 'false_positive' || normalized === 'fp') {
        return 'false_positive';
      }
      if (normalized === '정탐' || normalized === 'true_positive' || normalized === 'tp') {
        return 'true_positive';
      }
      return null;
    }
    function setCsvImportStatus(message) {
      const status = document.getElementById('csvImportStatus');
      if (status) {
        status.textContent = message;
      }
    }
    function importReviewDelimitedText(text, delimiter, formatLabel) {
      const rows = parseDelimitedText(text, delimiter).filter(row => row.some(value => !isBlank(value)));
      if (rows.length < 2) {
        setCsvImportStatus(`반영할 ${formatLabel} 행이 없습니다.`);
        return;
      }
      const headers = rows[0].map(header => String(header ?? '').trim());
      const headerIndex = new Map(headers.map((header, index) => [header, index]));
      if (!headerIndex.has('finding_key')) {
        setCsvImportStatus(`finding_key 컬럼이 없어 ${formatLabel}를 반영하지 못했습니다.`);
        return;
      }
      let applied = 0;
      let skipped = 0;
      let invalid = 0;
      rows.slice(1).forEach(row => {
        const key = String(row[headerIndex.get('finding_key')] ?? '').trim();
        if (!findingIndexByKey.has(key)) {
          skipped += 1;
          return;
        }
        const decisionColumn = headerIndex.get(ReviewCsvEditableHeaders.decision);
        const importedDecision = decisionColumn === undefined ? undefined : normalizeImportedDecision(row[decisionColumn]);
        if (importedDecision === null) {
          invalid += 1;
          return;
        }
        const index = findingIndexByKey.get(key);
        if (importedDecision !== undefined) {
          updateFormState(index, 'decision', importedDecision);
        }
        ['false_positive_reason', 'action_plan', 'action_due_date'].forEach(field => {
          const column = headerIndex.get(ReviewCsvEditableHeaders[field]);
          if (column !== undefined) {
            updateFormState(index, field, String(row[column] ?? ''));
          }
        });
        ['decision', 'false_positive_reason', 'action_plan', 'action_due_date'].forEach(field => {
          clearValidationField(index, field);
        });
        applied += 1;
      });
      if (applied > 0) {
        renderFindings();
      }
      const parts = [`${applied}건 반영`];
      if (skipped > 0) {
        parts.push(`${skipped}건 finding_key 불일치`);
      }
      if (invalid > 0) {
        parts.push(`${invalid}건 판정값 오류`);
      }
      setCsvImportStatus(parts.join(', '));
    }
    function importReviewCsvText(text) {
      importReviewDelimitedText(text, ',', 'CSV');
    }
    function importReviewTsvText(text) {
      importReviewDelimitedText(text, '\t', 'TSV');
    }
    function handleReviewCsvFile(event) {
      const file = event.target.files && event.target.files[0];
      if (!file) {
        return;
      }
      const reader = new FileReader();
      reader.onload = () => {
        importReviewCsvText(String(reader.result ?? ''));
        event.target.value = '';
      };
      reader.onerror = () => {
        setCsvImportStatus('CSV 파일을 읽지 못했습니다.');
        event.target.value = '';
      };
      reader.readAsText(file, 'utf-8');
    }
    function importPastedReviewTsv() {
      const textarea = document.getElementById('pasteReviewTsv');
      if (!textarea) {
        return;
      }
      if (isBlank(textarea.value)) {
        setCsvImportStatus('붙여넣은 TSV 내용이 없습니다.');
        return;
      }
      importReviewTsvText(textarea.value);
    }
    function renderExistingActionCell(finding) {
      const state = finding.action_plan_state;
      if (!state) {
        return '<span class="existing-action-empty">-</span>';
      }
      const status = String(state.status || 'remediation_planned').replace(/[^a-z_]/g, '');
      return `
        <span class="action-status-badge action-status-${escapeHtml(status)}">${escapeHtml(state.status_label || '조치 필요')}</span>
        <div class="existing-action-detail">계획: ${escapeHtml(state.action_plan || '-')}</div>
        <div class="existing-action-detail">예정일: ${escapeHtml(state.action_due_date || '-')}</div>
        <div class="existing-action-detail">응답자사번: ${escapeHtml(state.responder || '-')}</div>`;
    }
    function renderFindingCells(finding, index) {
      return `
        <td>${escapeHtml(finding.file_identifier)}<span hidden data-finding-key="${escapeHtml(finding.finding_key)}">${escapeHtml(finding.finding_key)}</span></td>
        <td>${escapeHtml(finding.hive_table_fqn)}</td>
        <td>${escapeHtml(finding.column_name)}</td>
        <td>${escapeHtml(displayPiiType(finding.pii_type))}</td>
        <td class="metric-cell">${escapeHtml(finding.sampled_row_count)}</td>
        <td class="metric-cell">${escapeHtml(finding.match_count)}</td>
        <td class="metric-cell">${escapeHtml(formatDetectionPercent(finding))}</td>
        <td class="sample">${renderSampleCell(finding)}</td>
        <td>
          <div class="decision-toggle" role="group" aria-label="판정" data-validation-field="decision" aria-invalid="false">
            <button type="button" class="decision-button" data-index="${index}" data-decision-button="false_positive" aria-pressed="false">오탐</button>
            <button type="button" class="decision-button" data-index="${index}" data-decision-button="true_positive" aria-pressed="false">정탐</button>
          </div>
        </td>
        <td class="existing-action-cell">${renderExistingActionCell(finding)}</td>
        <td class="reason-cell">
          <div class="decision-fields" data-decision-section="false_positive">
            <textarea data-index="${index}" data-field="false_positive_reason" aria-label="오탐 사유" placeholder="필수"></textarea>
          </div>
        </td>
        <td class="plan-cell">
          <div class="decision-fields" data-decision-section="true_positive">
            <textarea data-index="${index}" data-field="action_plan" aria-label="정탐 조치 계획" placeholder="필수"></textarea>
          </div>
        </td>
        <td class="date-cell">
          <div class="decision-fields" data-decision-section="true_positive">
            <input data-index="${index}" data-field="action_due_date" type="date" aria-label="조치 예정일" placeholder="YYYY-MM-DD" min="${todayDateOnly()}" max="${maxActionDueDate()}">
          </div>
        </td>`;
    }
    function hydrateRow(row) {
      if (!row || row.getAttribute('data-hydrated') === 'true') {
        return;
      }
      const index = Number(row.getAttribute('data-index'));
      const finding = REVIEW_DATA.findings[index];
      row.innerHTML = renderFindingCells(finding, index);
      row.setAttribute('data-hydrated', 'true');
      setFieldValues(row, index);
      applyDecisionVisibility(row);
      applyScopeVisibility(row);
      applyValidationState(row, index);
      hydratedRows.set(index, row);
    }
    function dehydrateRow(row) {
      if (!row || row.getAttribute('data-hydrated') !== 'true') {
        return;
      }
      const index = Number(row.getAttribute('data-index'));
      row.innerHTML = renderPlaceholderRow(index);
      row.setAttribute('data-hydrated', 'false');
      hydratedRows.delete(index);
    }
    function updateHydratedRow(index) {
      const row = hydratedRows.get(Number(index));
      if (row) {
        setFieldValues(row, index);
        applyDecisionVisibility(row);
        applyScopeVisibility(row);
        applyValidationState(row, index);
      }
    }
    function resetRowObserver() {
      hydratedRows.clear();
      if (rowObserver) {
        rowObserver.disconnect();
      }
      rowObserver = 'IntersectionObserver' in window
        ? new IntersectionObserver(entries => {
          entries.forEach(entry => {
            if (entry.isIntersecting) {
              hydrateRow(entry.target);
            } else {
              dehydrateRow(entry.target);
            }
          });
        }, { root: null, rootMargin: '1000px 0px', threshold: 0 })
        : null;
    }
    function observeRow(row) {
      if (rowObserver) {
        rowObserver.observe(row);
      } else {
        hydrateRow(row);
      }
    }
    function renderFindings() {
      resetRowObserver();
      const fragment = document.createDocumentFragment();
      sortRows(REVIEW_DATA.findings.map((_, index) => index)).forEach(index => {
        const row = document.createElement('tr');
        row.setAttribute('data-index', String(index));
        row.setAttribute('data-hydrated', 'false');
        row.innerHTML = renderPlaceholderRow(index);
        fragment.appendChild(row);
      });
      tbody.replaceChildren(fragment);
      tbody.querySelectorAll('tr[data-index]').forEach(observeRow);
      updateSortHeaders();
    }
    document.querySelectorAll('#findingsTable th[data-sort-key] button').forEach(button => {
      button.addEventListener('click', () => {
        const key = button.closest('th').getAttribute('data-sort-key');
        sortState = {
          key,
          direction: sortState.key === key && sortState.direction === 'asc' ? 'desc' : 'asc'
        };
        renderFindings();
      });
    });
    function handleDecisionClick(event) {
      const button = event.target.closest('[data-decision-button]');
      if (!button) {
        return;
      }
      const index = button.getAttribute('data-index');
      const decision = button.getAttribute('data-decision-button');
      const currentDecision = getFormState(index).decision;
      updateFormState(index, 'decision', currentDecision === decision ? '' : decision);
      clearValidationField(index, 'decision');
      clearValidationField(index, 'false_positive_reason');
      clearValidationField(index, 'action_plan');
      clearValidationField(index, 'action_due_date');
      const row = button.closest('tr');
      updateDecisionButtons(row, index);
      applyDecisionVisibility(row);
      applyScopeVisibility(row);
      applyValidationState(row, index);
    }
    function handleFormEvent(event) {
      if (!event.target.matches('[data-field]')) {
        return;
      }
      const input = event.target;
      const index = input.getAttribute('data-index');
      const field = input.getAttribute('data-field');
      updateFormState(index, field, input.value);
      if (!isBlank(input.value)) {
        clearValidationField(index, field);
      }
    }
    tbody.addEventListener('click', handleDecisionClick);
    tbody.addEventListener('input', handleFormEvent);
    tbody.addEventListener('change', handleFormEvent);
    document.getElementById('applyBulkTruePositivePlan').addEventListener('click', applyBulkTruePositivePlan);
    document.getElementById('applyBulkFalsePositiveReason').addEventListener('click', applyBulkFalsePositiveReason);
    document.getElementById('downloadReviewCsv').addEventListener('click', downloadReviewCsv);
    document.getElementById('importReviewCsv').addEventListener('change', handleReviewCsvFile);
    document.getElementById('importPastedReviewTsv').addEventListener('click', importPastedReviewTsv);
    responderInput.addEventListener('input', () => {
      if (ResponderPattern.test(responderInput.value.trim())) {
        clearResponderValidation();
      }
    });
    renderFindings();
    document.getElementById('downloadResponse').addEventListener('click', () => {
      const values = collectFormValues();
      const responses = REVIEW_DATA.findings.map((finding, index) => {
        const response = Object.assign({
          finding_key: finding.finding_key,
          finding_hash: finding.finding_hash,
          file_identifier: finding.file_identifier,
          file_identifier_pattern: finding.hive_table_fqn ? '' : finding.file_identifier,
          hive_database: finding.hive_database,
          hive_table: finding.hive_table,
          hive_table_fqn: finding.hive_table_fqn,
          column_name: finding.column_name,
          pii_type: finding.pii_type,
          sample_row_count: finding.sampled_row_count,
          match_count: finding.match_count,
          non_empty_match_ratio: finding.non_empty_match_ratio
        }, values[index] || {});
        return response;
      });
      const responderIsValid = validateResponder();
      const responseValidationErrors = validateResponses(responses);
      setValidationState(responseValidationErrors);
      if (!responderIsValid) {
        return;
      }
      if (responseValidationErrors.length > 0) {
        focusFirstValidationError(firstValidationErrorInDisplayOrder(responseValidationErrors));
        return;
      }
      const sanitizedResponses = responses.map(sanitizeResponse).filter(response => response.decision);
      const envelope = {
        schema_version: 1,
        scan_path: REVIEW_DATA.scan_path,
        responder: responderInput.value.trim(),
        responded_at: new Date().toISOString(),
        responses: sanitizedResponses
      };
      const blob = new Blob([JSON.stringify(envelope, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `response-${formatResponseScanPath(REVIEW_DATA.scan_path)}-${formatResponseTimestamp(new Date())}.json`;
      link.click();
      URL.revokeObjectURL(url);
    });
