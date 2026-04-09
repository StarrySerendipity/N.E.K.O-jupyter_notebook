const API_BASE = "";
const MAX_HIGH_CONCURRENT = 2;
const MAX_LOW_CONCURRENT = 1;
const HIGH_PRIORITY_THRESHOLD = 90;
const PARSE_CHUNK_CHARS = 64 * 1024;
const CACHE_MAX_NOTEBOOKS = 2;

const highQueue = [];
const lowQueue = [];
let highActive = 0;
let lowActive = 0;
const notebookOutlineCache = new Map();

try {
  importScripts("./vendor/clarinet.js");
} catch (_err) {
  // Optional dependency: worker keeps fallback behavior even if parser lib fails to load.
}

function authHeaders(token) {
  return {
    "Content-Type": "application/json",
    Authorization: token ? `Bearer ${token}` : "",
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toPriority(payload) {
  return Number(payload?.priority || 0);
}

function isHighPriority(task) {
  return toPriority(task?.payload) >= HIGH_PRIORITY_THRESHOLD;
}

async function maybeEncodeResult(result) {
  try {
    const text = JSON.stringify(result || {});
    if (text.length < 8192) {
      return { type: "result", result: result || {} };
    }

    const bytes = new TextEncoder().encode(text);
    if (typeof CompressionStream === "undefined") {
      return { type: "result", result: result || {} };
    }

    const stream = new Blob([bytes]).stream().pipeThrough(new CompressionStream("gzip"));
    const compressed = await new Response(stream).arrayBuffer();
    return {
      type: "result-bin",
      encoding: "gzip-json",
      payload: compressed,
    };
  } catch (_err) {
    return { type: "result", result: result || {} };
  }
}

function getClarinetParser() {
  if (self && self.clarinet && typeof self.clarinet.parser === "function") {
    return self.clarinet.parser();
  }
  return null;
}

function parseNotebookHeaderWithClarinet(rawText) {
  const fallback = { nbformat: 4, nbformat_minor: 5 };
  const parser = getClarinetParser();
  if (!parser || typeof rawText !== "string" || !rawText.length) {
    return fallback;
  }

  let nbformat = 4;
  let nbformatMinor = 5;
  const stack = [];
  let currentKey = "";

  parser.onerror = function (_err) {
    try {
      parser.error = null;
      if (typeof parser.resume === "function") parser.resume();
    } catch (_e) {
      // Ignore parse recovery failures.
    }
  };

  parser.onopenobject = function (key) {
    stack.push("object");
    currentKey = typeof key === "string" ? key : "";
  };
  parser.onkey = function (key) {
    currentKey = typeof key === "string" ? key : "";
  };
  parser.onopenarray = function () {
    stack.push("array");
  };
  parser.oncloseobject = function () {
    stack.pop();
    currentKey = "";
  };
  parser.onclosearray = function () {
    stack.pop();
    currentKey = "";
  };
  parser.onvalue = function (value) {
    if (stack.length === 1 && stack[0] === "object") {
      if (currentKey === "nbformat") {
        const num = Number(value);
        if (Number.isFinite(num)) nbformat = Math.max(1, Math.floor(num));
      } else if (currentKey === "nbformat_minor") {
        const num = Number(value);
        if (Number.isFinite(num)) nbformatMinor = Math.max(0, Math.floor(num));
      }
    }
  };

  try {
    for (let i = 0; i < rawText.length; i += PARSE_CHUNK_CHARS) {
      parser.write(rawText.slice(i, i + PARSE_CHUNK_CHARS));
    }
    parser.close();
  } catch (_err) {
    return fallback;
  }

  return {
    nbformat: nbformat || 4,
    nbformat_minor: nbformatMinor || 5,
  };
}

function sourceToText(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item == null ? "" : item)).join("");
  }
  return String(value == null ? "" : value);
}

function scanJsonStringEnd(raw, startQuote) {
  let i = Math.max(0, Number(startQuote || 0)) + 1;
  let escaped = false;
  while (i < raw.length) {
    const ch = raw.charCodeAt(i);
    if (escaped) {
      escaped = false;
      i += 1;
      continue;
    }
    if (ch === 92) {
      escaped = true;
      i += 1;
      continue;
    }
    if (ch === 34) {
      return i + 1;
    }
    i += 1;
  }
  return -1;
}

function findMatchingBracket(raw, start, openCh, closeCh, limitEnd = -1) {
  let i = Number(start || 0);
  const limit = Number(limitEnd || 0) > 0 ? Number(limitEnd) : raw.length;
  if (i < 0 || i >= raw.length || raw[i] !== openCh) {
    return -1;
  }

  let depth = 0;
  let inString = false;
  let escaped = false;

  while (i < limit) {
    const ch = raw.charCodeAt(i);
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === 92) {
        escaped = true;
      } else if (ch === 34) {
        inString = false;
      }
      i += 1;
      continue;
    }

    if (ch === 34) {
      inString = true;
      i += 1;
      continue;
    }

    const chr = raw[i];
    if (chr === openCh) {
      depth += 1;
    } else if (chr === closeCh) {
      depth -= 1;
      if (depth === 0) {
        return i;
      }
    }
    i += 1;
  }
  return -1;
}

function scanJsonValueEnd(raw, start, limitEnd = -1) {
  let i = Number(start || 0);
  const limit = Number(limitEnd || 0) > 0 ? Number(limitEnd) : raw.length;
  if (i < 0 || i >= limit) return -1;

  while (i < limit && /\s/.test(raw[i])) i += 1;
  if (i >= limit) return -1;

  const ch = raw[i];
  if (ch === '"') {
    return scanJsonStringEnd(raw, i);
  }
  if (ch === "{") {
    const end = findMatchingBracket(raw, i, "{", "}", limit);
    return end < 0 ? -1 : end + 1;
  }
  if (ch === "[") {
    const end = findMatchingBracket(raw, i, "[", "]", limit);
    return end < 0 ? -1 : end + 1;
  }

  while (i < limit) {
    const c = raw[i];
    if (c === "," || c === "}" || c === "]") {
      break;
    }
    i += 1;
  }
  return i;
}

function extractTopLevelValueSpanInObject(raw, objectStart, objectEnd, key) {
  const start = Math.max(0, Number(objectStart || 0));
  const endLimit = Math.min(raw.length, Number(objectEnd || raw.length));
  if (start >= endLimit || raw[start] !== "{") {
    return null;
  }

  let i = start + 1;
  while (i < endLimit) {
    while (i < endLimit && /\s/.test(raw[i])) i += 1;
    if (i >= endLimit) break;
    if (raw[i] === "}") break;
    if (raw[i] === ",") {
      i += 1;
      continue;
    }
    if (raw[i] !== '"') {
      i += 1;
      continue;
    }

    const keyStart = i;
    const keyEnd = scanJsonStringEnd(raw, keyStart);
    if (keyEnd < 0 || keyEnd > endLimit) {
      break;
    }

    let parsedKey = "";
    try {
      parsedKey = String(JSON.parse(raw.slice(keyStart, keyEnd)));
    } catch (_err) {
      parsedKey = "";
    }

    i = keyEnd;
    while (i < endLimit && /\s/.test(raw[i])) i += 1;
    if (i >= endLimit || raw[i] !== ":") {
      continue;
    }

    i += 1;
    while (i < endLimit && /\s/.test(raw[i])) i += 1;
    const valueStart = i;
    const valueEnd = scanJsonValueEnd(raw, valueStart, endLimit);
    if (valueEnd < 0) break;

    if (parsedKey === key) {
      return {
        start: valueStart,
        end: valueEnd,
      };
    }
    i = valueEnd;
  }

  return null;
}

function extractTopLevelValueSpansInObject(raw, objectStart, objectEnd, keys) {
  const wanted = new Set(Array.isArray(keys) ? keys.map((k) => String(k || "")) : []);
  const spans = {};

  const start = Math.max(0, Number(objectStart || 0));
  const endLimit = Math.min(raw.length, Number(objectEnd || raw.length));
  if (!wanted.size || start >= endLimit || raw[start] !== "{") {
    return spans;
  }

  let i = start + 1;
  while (i < endLimit) {
    while (i < endLimit && /\s/.test(raw[i])) i += 1;
    if (i >= endLimit) break;
    if (raw[i] === "}") break;
    if (raw[i] === ",") {
      i += 1;
      continue;
    }
    if (raw[i] !== '"') {
      i += 1;
      continue;
    }

    const keyStart = i;
    const keyEnd = scanJsonStringEnd(raw, keyStart);
    if (keyEnd < 0 || keyEnd > endLimit) {
      break;
    }

    let parsedKey = "";
    try {
      parsedKey = String(JSON.parse(raw.slice(keyStart, keyEnd)));
    } catch (_err) {
      parsedKey = "";
    }

    i = keyEnd;
    while (i < endLimit && /\s/.test(raw[i])) i += 1;
    if (i >= endLimit || raw[i] !== ":") {
      continue;
    }

    i += 1;
    while (i < endLimit && /\s/.test(raw[i])) i += 1;
    const valueStart = i;
    const valueEnd = scanJsonValueEnd(raw, valueStart, endLimit);
    if (valueEnd < 0) {
      break;
    }

    if (wanted.has(parsedKey) && !spans[parsedKey]) {
      spans[parsedKey] = {
        start: valueStart,
        end: valueEnd,
      };
      if (Object.keys(spans).length >= wanted.size) {
        return spans;
      }
    }
    i = valueEnd;
  }

  return spans;
}

function decodeJsonValueSafe(rawValue) {
  const value = String(rawValue == null ? "" : rawValue).trim();
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch (_err) {
    return null;
  }
}

function normalizeMetadata(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? { ...value } : {};
}

function scanCellsRangesFromText(raw) {
  const rootStart = raw.indexOf("{");
  if (rootStart < 0) {
    throw new Error("invalid notebook: root object missing");
  }

  const cellsSpan = extractTopLevelValueSpanInObject(raw, rootStart, raw.length, "cells");
  if (!cellsSpan) {
    throw new Error("invalid notebook: cells key missing");
  }

  let arrStart = cellsSpan.start;
  while (arrStart < raw.length && /\s/.test(raw[arrStart])) arrStart += 1;
  if (arrStart >= raw.length || raw[arrStart] !== "[") {
    throw new Error("invalid notebook: cells is not an array");
  }

  const arrEnd = findMatchingBracket(raw, arrStart, "[", "]");
  if (arrEnd < 0) {
    throw new Error("invalid notebook: cells array not closed");
  }

  const ranges = [];
  let depth = 0;
  let inString = false;
  let escaped = false;
  let objectStart = -1;

  for (let i = arrStart + 1; i < arrEnd; i += 1) {
    const ch = raw.charCodeAt(i);
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === 92) {
        escaped = true;
      } else if (ch === 34) {
        inString = false;
      }
      continue;
    }

    if (ch === 34) {
      inString = true;
      continue;
    }

    const chr = raw[i];
    if (chr === "{") {
      if (depth === 0) {
        objectStart = i;
      }
      depth += 1;
    } else if (chr === "}") {
      depth -= 1;
      if (depth === 0 && objectStart >= 0) {
        ranges.push({ start: objectStart, end: i + 1 });
        objectStart = -1;
      }
    }
  }

  return ranges;
}

function buildCellOutlineFromRaw(raw, range, index, notebookPath) {
  const start = Number(range?.start || 0);
  const end = Number(range?.end || start);
  const spans = extractTopLevelValueSpansInObject(raw, start, end, [
    "metadata",
    "id",
    "cell_type",
    "execution_count",
    "source",
    "outputs",
  ]);
  const metadataSpan = spans.metadata || null;
  const metadataVal = metadataSpan ? decodeJsonValueSafe(raw.slice(metadataSpan.start, metadataSpan.end)) : {};
  const metadata = normalizeMetadata(metadataVal);

  const idSpan = spans.id || null;
  const idVal = idSpan ? decodeJsonValueSafe(raw.slice(idSpan.start, idSpan.end)) : null;
  const id = String(metadata.id || idVal || `cell-${index + 1}`);
  metadata.id = id;

  const typeSpan = spans.cell_type || null;
  const typeVal = typeSpan ? decodeJsonValueSafe(raw.slice(typeSpan.start, typeSpan.end)) : "markdown";
  const cellType = String(typeVal || "").toLowerCase() === "code" ? "code" : "markdown";

  const execSpan = spans.execution_count || null;
  const execVal = execSpan ? decodeJsonValueSafe(raw.slice(execSpan.start, execSpan.end)) : null;
  const executionCount = Number.isInteger(execVal) ? Number(execVal) : null;

  const sourceSpan = spans.source || null;
  const outputsSpan = spans.outputs || null;

  let hasOutput = false;
  let hasVideoOutput = false;
  let outputCount = 0;
  if (outputsSpan && outputsSpan.end > outputsSpan.start) {
    const spanLen = outputsSpan.end - outputsSpan.start;
    const probe = raw.slice(outputsSpan.start, Math.min(outputsSpan.end, outputsSpan.start + 1024)).replace(/\s+/g, "");
    hasOutput = !(spanLen <= 4 && (probe === "[]" || probe === "[ ]" || probe === ""));
    if (hasOutput) {
      outputCount = 1;
      const mediaProbe = raw.slice(outputsSpan.start, Math.min(outputsSpan.end, outputsSpan.start + 8192)).toLowerCase();
      hasVideoOutput = mediaProbe.includes('"video/') || mediaProbe.includes('"video\\/');
    }
  }

  return {
    index,
    id,
    cell_type: cellType,
    metadata,
    source: "",
    outputs: [],
    output_text: "",
    execution_count: executionCount,
    outline_only: true,
    details_loaded: false,
    source_line_count: 1,
    has_output: hasOutput,
    output_count: outputCount,
    has_video_output: hasVideoOutput,
    primary_media_mime: hasVideoOutput ? "video/mp4" : "",
    primary_media_size_bytes: 0,
    notebook_path: notebookPath,
    cell_start_offset: start,
    cell_end_offset: end,
    source_start_offset: sourceSpan ? sourceSpan.start : -1,
    source_end_offset: sourceSpan ? sourceSpan.end : -1,
    outputs_start_offset: outputsSpan ? outputsSpan.start : -1,
    outputs_end_offset: outputsSpan ? outputsSpan.end : -1,
  };
}

function buildOutlineFromRawText(rawText, notebookPath, fileSizeBytes) {
  const ranges = scanCellsRangesFromText(rawText);
  const header = parseNotebookHeaderWithClarinet(rawText);
  const cells = [];
  for (let i = 0; i < ranges.length; i += 1) {
    cells.push(buildCellOutlineFromRaw(rawText, ranges[i], i, notebookPath));
  }

  return {
    path: notebookPath,
    cells,
    total_cell_count: cells.length,
    loaded_cell_offset: 0,
    loaded_cell_count: cells.length,
    metadata: {},
    nbformat: Number(header.nbformat || 4),
    nbformat_minor: Number(header.nbformat_minor || 5),
    outline_only: true,
    file_size_bytes: Math.max(0, Number(fileSizeBytes || 0)),
  };
}

function buildCellDetailFromCache(rawText, notebookPath, index, outlineCell) {
  const idx = Math.max(0, Number(index || 0));
  const start = Number(outlineCell?.cell_start_offset || -1);
  const end = Number(outlineCell?.cell_end_offset || -1);
  if (start < 0 || end <= start || end > rawText.length) {
    throw new Error("cell range invalid");
  }

  const cellRaw = rawText.slice(start, end);
  const parsed = JSON.parse(cellRaw);
  const cellObj = parsed && typeof parsed === "object" ? parsed : {};

  const metadata = normalizeMetadata(cellObj.metadata);
  const id = String(cellObj.id || metadata.id || outlineCell?.id || `cell-${idx + 1}`);
  metadata.id = id;

  const cellType = String(cellObj.cell_type || outlineCell?.cell_type || "markdown").toLowerCase() === "code" ? "code" : "markdown";
  const sourceText = sourceToText(cellObj.source);
  const outputs = Array.isArray(cellObj.outputs) ? cellObj.outputs : [];
  const executionCount = Number.isInteger(cellObj.execution_count) ? Number(cellObj.execution_count) : null;
  const sourceLineCount = Math.max(1, sourceText ? sourceText.split(/\r\n|\r|\n/).length : 1);

  return {
    index: idx,
    id,
    cell_type: cellType,
    metadata,
    source: sourceText,
    outputs,
    output_text: "",
    execution_count: executionCount,
    outline_only: false,
    details_loaded: true,
    source_line_count: sourceLineCount,
    has_output: outputs.length > 0,
    output_count: outputs.length,
    notebook_path: notebookPath,
  };
}

function putNotebookCache(path, payload) {
  const key = String(path || "");
  if (!key) return;
  notebookOutlineCache.set(key, payload);
  while (notebookOutlineCache.size > CACHE_MAX_NOTEBOOKS) {
    const first = notebookOutlineCache.keys().next();
    if (first && !first.done) {
      notebookOutlineCache.delete(first.value);
    } else {
      break;
    }
  }
}

function runEntryPayload(base, entryId, args, options = {}) {
  return {
    pluginId: String(base?.pluginId || ""),
    entryId: String(entryId || ""),
    args: args && typeof args === "object" ? args : {},
    token: String(base?.token || ""),
    pollIntervalMs: Math.max(80, Number(options.pollIntervalMs || base?.pollIntervalMs || 180)),
    maxWaitMs: Math.max(1000, Number(options.maxWaitMs || base?.maxWaitMs || 90000)),
    priority: Number(options.priority || 75),
  };
}

async function runEntryOnce(basePayload, entryId, args, options = {}) {
  return executeRunEntry({
    payload: runEntryPayload(basePayload, entryId, args, options),
  });
}

async function runParseNotebookOutlineTask(payload) {
  const notebookPath = String(payload?.notebookPath || "");
  if (!notebookPath) {
    return { success: false, data: {}, error: "missing notebookPath" };
  }

  const existing = notebookOutlineCache.get(notebookPath);
  if (existing && existing.outline) {
    return {
      success: true,
      data: {
        notebook_path: notebookPath,
        cell_count: Number(existing.outline.total_cell_count || 0),
        total_cell_count: Number(existing.outline.total_cell_count || 0),
        notebook: existing.outline,
      },
      error: "",
    };
  }

  const rawResp = await runEntryOnce(
    payload,
    "read_notebook_raw_text",
    { notebook_path: notebookPath },
    { maxWaitMs: 300000, pollIntervalMs: 240, priority: 82 },
  );
  if (!rawResp || rawResp.success === false) {
    return {
      success: false,
      data: {},
      error: String(rawResp?.error || "read raw notebook text failed"),
    };
  }

  const data = rawResp.data && typeof rawResp.data === "object" ? rawResp.data : {};
  const resolvedPath = String(data.notebook_path || notebookPath);
  const rawText = String(data.text || "");
  const fileSize = Number(data.file_size_bytes || 0);
  const outline = buildOutlineFromRawText(rawText, resolvedPath, fileSize);

  putNotebookCache(resolvedPath, {
    path: resolvedPath,
    rawText,
    outline,
    fileSizeBytes: fileSize,
    updatedAt: Date.now(),
  });

  return {
    success: true,
    data: {
      notebook_path: resolvedPath,
      cell_count: Number(outline.total_cell_count || 0),
      total_cell_count: Number(outline.total_cell_count || 0),
      notebook: outline,
    },
    error: "",
  };
}

async function runLoadCellDetailTask(payload) {
  const notebookPath = String(payload?.notebookPath || "");
  const cellIndex = Number(payload?.cellIndex);
  if (!notebookPath || !Number.isInteger(cellIndex) || cellIndex < 0) {
    return { success: false, data: {}, error: "invalid notebookPath or cellIndex" };
  }

  let cached = notebookOutlineCache.get(notebookPath);
  if (!cached || !cached.outline || typeof cached.rawText !== "string") {
    const prep = await runParseNotebookOutlineTask(payload);
    if (!prep.success) {
      return prep;
    }
    cached = notebookOutlineCache.get(notebookPath);
  }

  if (!cached || !cached.outline || typeof cached.rawText !== "string") {
    return { success: false, data: {}, error: "outline cache unavailable" };
  }

  const cells = Array.isArray(cached.outline.cells) ? cached.outline.cells : [];
  if (cellIndex >= cells.length) {
    return { success: false, data: {}, error: "cell index out of range" };
  }

  try {
    const detailCell = buildCellDetailFromCache(cached.rawText, notebookPath, cellIndex, cells[cellIndex]);
    return {
      success: true,
      data: {
        notebook_path: notebookPath,
        cell_index: cellIndex,
        total_cell_count: Number(cached.outline.total_cell_count || cells.length),
        notebook: {
          path: notebookPath,
          cells: [detailCell],
          total_cell_count: Number(cached.outline.total_cell_count || cells.length),
          loaded_cell_offset: cellIndex,
          loaded_cell_count: 1,
          metadata: cached.outline.metadata || {},
          nbformat: Number(cached.outline.nbformat || 4),
          nbformat_minor: Number(cached.outline.nbformat_minor || 5),
        },
      },
      error: "",
    };
  } catch (err) {
    return {
      success: false,
      data: {},
      error: String(err?.message || err || "load cell detail failed"),
    };
  }
}

async function readRunExport(runId, runStatus, token) {
  try {
    const exportResp = await fetch(`${API_BASE}/runs/${runId}/export`, { headers: authHeaders(token) });
    if (exportResp.ok) {
      const exportObj = await exportResp.json();
      const items = Array.isArray(exportObj.items) ? exportObj.items : [];
      for (const item of items) {
        let payload = null;
        if (item && item.type === "json" && (item.json != null || item.json_data != null)) {
          payload = item.json ?? item.json_data;
        } else if (item && item.type === "text" && typeof item.text === "string") {
          try {
            payload = JSON.parse(item.text);
          } catch (_err) {
            payload = null;
          }
        }

        if (!payload) continue;

        const pluginResp = payload.plugin_response || payload;
        if (pluginResp && typeof pluginResp === "object") {
          const success = pluginResp.success !== false && runStatus === "succeeded";
          const data = pluginResp.data || {};
          const error = pluginResp.error?.message || pluginResp.error || (success ? "" : "task failed");
          return { success, data, error };
        }
      }
    }
  } catch (_err) {
    // Fall through to status-based fallback.
  }

  return {
    success: runStatus === "succeeded",
    data: {},
    error: runStatus === "failed" ? "plugin run failed" : "",
  };
}

async function executeRunEntry(task) {
  const payload = task.payload || {};
  const token = String(payload.token || "");
  const pollIntervalMs = Math.max(80, Number(payload.pollIntervalMs || 180));
  const maxWaitMs = Math.max(pollIntervalMs, Number(payload.maxWaitMs || 90000));

  const createResp = await fetch(`${API_BASE}/runs`, {
    method: "POST",
    headers: authHeaders(token),
    body: JSON.stringify({
      plugin_id: String(payload.pluginId || ""),
      entry_id: String(payload.entryId || ""),
      args: payload.args && typeof payload.args === "object" ? payload.args : {},
    }),
  });

  if (!createResp.ok) {
    throw new Error(`create run failed: HTTP ${createResp.status}`);
  }

  const run = await createResp.json();
  const runId = String(run.run_id || "");
  if (!runId) {
    throw new Error("missing run_id");
  }

  const startedAt = Date.now();
  while (Date.now() - startedAt <= maxWaitMs) {
    const statusResp = await fetch(`${API_BASE}/runs/${runId}`, { headers: authHeaders(token) });
    if (!statusResp.ok) {
      throw new Error(`run status failed: HTTP ${statusResp.status}`);
    }

    const statusObj = await statusResp.json();
    const status = String(statusObj.status || "");
    if (status === "succeeded" || status === "failed") {
      return readRunExport(runId, status, token);
    }

    await sleep(pollIntervalMs);
  }

  return { success: false, data: {}, error: "等待运行结果超时" };
}

function enqueue(task) {
  if (!task || !task.payload) return;

  const entryId = String(task.payload.entryId || "");
  const args = task.payload.args && typeof task.payload.args === "object" ? task.payload.args : {};
  const cellId = String(args.cell_id || "");

  if (entryId === "update_cell" && cellId) {
    for (let i = lowQueue.length - 1; i >= 0; i -= 1) {
      const queued = lowQueue[i];
      const queuedEntry = String(queued?.payload?.entryId || "");
      const queuedCellId = String(queued?.payload?.args?.cell_id || "");
      if (queuedEntry === "update_cell" && queuedCellId === cellId) {
        lowQueue.splice(i, 1);
      }
    }
  }

  const targetQueue = isHighPriority(task) ? highQueue : lowQueue;
  targetQueue.push(task);
  targetQueue.sort((a, b) => {
    const pa = Number(a.payload?.priority || 0);
    const pb = Number(b.payload?.priority || 0);
    if (pa !== pb) return pb - pa;
    return Number(a.createdAt || 0) - Number(b.createdAt || 0);
  });
  drainQueue();
}

function drainQueue() {
  while (highActive < MAX_HIGH_CONCURRENT && highQueue.length > 0) {
    const task = highQueue.shift();
    if (!task) continue;
    highActive += 1;

    executeRunEntry(task)
      .then(async (result) => {
        const encoded = await maybeEncodeResult(result);
        self.postMessage({
          ...encoded,
          requestId: task.requestId,
        }, encoded.payload instanceof ArrayBuffer ? [encoded.payload] : undefined);
      })
      .catch((err) => {
        self.postMessage({
          type: "error",
          requestId: task.requestId,
          error: String(err?.message || err || "worker task failed"),
        });
      })
      .finally(() => {
        highActive = Math.max(0, highActive - 1);
        drainQueue();
      });
  }

  while (lowActive < MAX_LOW_CONCURRENT && lowQueue.length > 0) {
    const task = lowQueue.shift();
    if (!task) continue;
    lowActive += 1;

    executeRunEntry(task)
      .then(async (result) => {
        const encoded = await maybeEncodeResult(result);
        self.postMessage({
          ...encoded,
          requestId: task.requestId,
        }, encoded.payload instanceof ArrayBuffer ? [encoded.payload] : undefined);
      })
      .catch((err) => {
        self.postMessage({
          type: "error",
          requestId: task.requestId,
          error: String(err?.message || err || "worker task failed"),
        });
      })
      .finally(() => {
        lowActive = Math.max(0, lowActive - 1);
        drainQueue();
      });
  }
}

self.addEventListener("message", (event) => {
  const data = event && event.data;
  if (!data || typeof data !== "object") return;
  const type = String(data.type || "");

  if (type === "runEntry") {
    const requestId = String(data.requestId || "");
    if (!requestId) return;
    enqueue({
      requestId,
      payload: data.payload || {},
      createdAt: Date.now(),
    });
    return;
  }

  if (type === "task") {
    const requestId = String(data.requestId || "");
    const payload = data.payload && typeof data.payload === "object" ? data.payload : {};
    const taskType = String(payload.taskType || "");
    if (!requestId || !taskType) return;

    (async () => {
      try {
        let result = { success: false, data: {}, error: "unsupported task" };
        if (taskType === "parseNotebookOutline") {
          result = await runParseNotebookOutlineTask(payload);
        } else if (taskType === "loadCellDetail") {
          result = await runLoadCellDetailTask(payload);
        }

        const encoded = await maybeEncodeResult(result);
        self.postMessage(
          {
            ...encoded,
            requestId,
          },
          encoded.payload instanceof ArrayBuffer ? [encoded.payload] : undefined,
        );
      } catch (err) {
        self.postMessage({
          type: "error",
          requestId,
          error: String(err?.message || err || "worker task failed"),
        });
      }
    })();
    return;
  }

  if (type === "clear") {
    highQueue.length = 0;
    lowQueue.length = 0;
    notebookOutlineCache.clear();
  }
});
