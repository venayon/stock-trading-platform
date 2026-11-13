import {requestJson, validateTrade} from './validation.mjs';

const form = document.querySelector('#trade-form');
const status = document.querySelector('#status');
const preview = document.querySelector('#preview');
const button = document.querySelector('#submit');
const values = () => Object.fromEntries(new FormData(form));
let submitted = false;

function showErrors(errors) {
  for (const field of form.elements) {
    const error = document.getElementById(`${field.name}-error`);
    if (!error) continue;
    error.textContent = errors[field.name] ?? '';
    field.setAttribute('aria-invalid', String(Boolean(errors[field.name])));
  }
}

form.addEventListener('input', () => {
  preview.hidden = true;
  if (submitted) showErrors(validateTrade(values()));
});
form.addEventListener('submit', async event => {
  event.preventDefault();
  submitted = true;
  preview.hidden = true;
  const input = values();
  const errors = validateTrade(input);
  showErrors(errors);
  if (Object.keys(errors).length) {
    status.textContent = 'Please correct the highlighted fields.';
    form.querySelector('[aria-invalid="true"]')?.focus();
    return;
  }
  button.disabled = true;
  status.textContent = 'Checking the API validation rules…';
  try {
    const response = await fetch('/api/trades/validate', {
      method: 'POST', headers: {'Content-Type': 'application/json', 'X-Correlation-ID': crypto.randomUUID()},
      body: requestJson(input)
    });
    const text = await response.text();
    if (!response.ok) {
      const problem = JSON.parse(text);
      showErrors(problem.errors ?? {});
      status.textContent = `${problem.detail ?? 'Validation failed.'} Reference: ${problem.correlationId ?? 'unavailable'}`;
      return;
    }
    status.textContent = 'Valid trade preview. No trade was saved or published.';
    // Render the original JSON text so 64-bit quantities are not rounded by JSON.parse.
    preview.textContent = text;
    preview.hidden = false;
  } catch {
    status.textContent = 'The validation API could not be reached. Please try again.';
  } finally {
    button.disabled = false;
  }
});
