const API_BASE = "";

function getPluginId() {
  const match = window.location.pathname.match(/\/plugin\/([^/]+)\/ui/);
  return match ? match[1] : "jupyter_notebook";
}

function makeLocalCellId() {
  return `local-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
}

const state = {
  pluginId: getPluginId(),
  notebooks: [],
  currentPath: "",
  currentNotebookSizeBytes: 0,
  largeNotebookMode: false,
  largeNotebookOutlineReady: false,
  largeNotebookInitialBatchPending: false,
  notebookDetailLoadProgress: 100,
  notebookDetailLoadLabel: "",
  notebook: null,
  uiInfo: null,
  cellStatus: {},
  sidebarTab: "files",
  sidebarCollapsed: false,
  selectedCellId: "",
  collapsedCellIds: new Set(),
  mobileToolbarExpanded: false,
  floatingPanelAction: "",
  floatingPrefs: null,
  floatingAiHistory: [],
  floatingAiDraft: "",
  speechRecognition: null,
  isSpeechListening: false,
  screenRecorder: null,
  recordingStream: null,
  recordedChunks: [],
  cellSyncPromises: new Map(),
  relayoutScheduled: false,
  notebookTotalCells: 0,
  notebookLoadPromise: null,
  notebookLoadToken: "",
  cellDetailHydrationPromises: new Map(),
  cellLastActiveAt: new Map(),
  outputExpandedCellIds: new Set(),
  mediaPayloadCache: new Map(),
  mediaPayloadSeq: 0,
  mediaObjectUrls: new Set(),
  virtualCellHeights: new Map(),
  virtualScrollScheduled: false,
  uiBusyLabel: "",
  worker: null,
  workerReady: false,
  workerSeq: 0,
  workerRequests: new Map(),
  mediaFsDirHandle: null,
};

const updateTimers = new Map();
let toastTimer = null;
let notebookListRefreshTimer = null;
let runShortcutBusy = false;
const DEFAULT_POLL_INTERVAL_MS = 180;
const DEFAULT_MAX_WAIT_MS = 90000;
const RUN_FAST_LANE_POLL_INTERVAL_MS = 40;
const NOTEBOOK_CHUNK_SIZE = 140;
const NOTEBOOK_LARGE_FILE_THRESHOLD_BYTES = 40 * 1024 * 1024;
const MEDIA_LAZY_THRESHOLD_BYTES = 5 * 1024 * 1024;
const INLINE_IMAGE_MAX_BYTES = 5 * 1024 * 1024;
const NOTEBOOK_LARGE_FIRST_BATCH_COUNT = 5;
const NOTEBOOK_INACTIVE_UNLOAD_MS = 5 * 60 * 1000;
const VIRTUAL_RENDER_THRESHOLD = 70;
const VIRTUAL_OVERSCAN = 5;

const UPLOAD_IMAGE_MIME_LIST = [
  "image/png",
  "image/jpeg",
  "image/gif",
  "image/webp",
  "image/bmp",
  "image/svg+xml",
];
const UPLOAD_VIDEO_MIME_LIST = [
  "video/mp4",
  "video/quicktime",
  "video/x-msvideo",
  "video/x-matroska",
  "video/x-flv",
  "video/x-ms-wmv",
  "video/webm",
];
const UPLOAD_IMAGE_EXT_LIST = [".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg"];
const UPLOAD_VIDEO_EXT_LIST = [".mp4", ".mov", ".avi", ".mkv", ".flv", ".wmv", ".webm"];

const runQueue = {
  pending: new Map(),
  timer: null,
  ticking: false,
};

const ENTRY_PRIORITY = {
  execute_cell: 100,
  delete_notebook: 97,
  rename_notebook: 96,
  delete_cell: 95,
  restart_kernel: 92,
  shutdown_kernel: 90,
  read_notebook_raw_text: 86,
  add_cell: 80,
  save_notebook: 70,
  load_notebook: 65,
  load_notebook_outline: 68,
  store_notebook_media_asset: 84,
  store_notebook_media_asset_from_path: 85,
  load_notebook_media_asset_payload: 82,
  list_notebooks: 60,
  get_ui_info: 55,
  update_cell: 40,
  read_clipboard_media_files: 65,
  send_ai_message: 75,
};

function formatBytes(bytes) {
  const size = Math.max(0, Number(bytes || 0));
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  if (size < 1024 * 1024 * 1024) return `${(size / (1024 * 1024)).toFixed(1)} MB`;
  return `${(size / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function getNotebookSizeByPath(path) {
  const key = String(path || "");
  if (!key) return 0;
  const item = (state.notebooks || []).find((nb) => String(nb?.path || "") === key);
  return Math.max(0, Number(item?.size || 0));
}

function isLargeNotebookBySize(sizeBytes) {
  return Number(sizeBytes || 0) >= NOTEBOOK_LARGE_FILE_THRESHOLD_BYTES;
}

function applyLargeNotebookAwareOptions(options = {}) {
  const next = { ...(options || {}) };
  if (!state.largeNotebookMode) return next;
  next.pollIntervalMs = Math.max(260, Number(next.pollIntervalMs || DEFAULT_POLL_INTERVAL_MS));
  next.maxWaitMs = Math.max(240000, Number(next.maxWaitMs || DEFAULT_MAX_WAIT_MS));
  return next;
}

function setUiBusy(label = "") {
  state.uiBusyLabel = String(label || "").trim();
  updateNotebookHeaderTitle();
}

function touchCellActivity(cellId) {
  const key = String(cellId || "");
  if (!key) return;
  state.cellLastActiveAt.set(key, Date.now());
}

function updateNotebookHeaderTitle() {
  const nameEl = document.getElementById("notebook-display-name");
  if (!nameEl) return;

  const path = String(state.currentPath || "未打开笔记本");
  const baseName = path.split("/").pop() || path;
  const chips = [];

  if (state.largeNotebookMode) {
    chips.push(`大文件模式 ${formatBytes(state.currentNotebookSizeBytes)}`);
    if (String(state.notebookDetailLoadLabel || "")) {
      chips.push(String(state.notebookDetailLoadLabel));
    }
  }

  if (String(state.uiBusyLabel || "")) {
    chips.push(String(state.uiBusyLabel));
  }

  nameEl.textContent = chips.length ? `${baseName} · ${chips.join(" · ")}` : baseName;
}

function hasCellOutput(cell) {
  return (Array.isArray(cell?.outputs) && cell.outputs.length > 0) || Boolean(String(cell?.output_text || "").trim()) || Boolean(cell?.has_output);
}

function isHeavyOutputCell(cell) {
  if (!hasCellOutput(cell)) return false;

  const outputs = Array.isArray(cell?.outputs) ? cell.outputs : [];
  if (outputs.length >= 3) return true;
  if (String(cell?.output_text || "").length >= 6000) return true;

  for (const item of outputs) {
    if (!item || typeof item !== "object") continue;
    const outputType = String(item.output_type || "");
    const meta = item.metadata && typeof item.metadata === "object" ? item.metadata : {};
    const mediaMime = String(meta.neko_media_mime || "").toLowerCase();
    const mediaSize = Number(meta.neko_media_size_bytes || 0);
    if (mediaMime.startsWith("video/") || mediaSize > MEDIA_LAZY_THRESHOLD_BYTES) {
      return true;
    }
    if (outputType === "stream" && readMimeAsText(item.text).length >= 4000) return true;
    if (outputType === "error") {
      const tb = Array.isArray(item.traceback) ? item.traceback.join("\n") : "";
      if (tb.length >= 2200) return true;
    }

    const data = item.data && typeof item.data === "object" ? item.data : {};
    if (
      data["application/vnd.plotly.v1+json"]
      || data["text/html"]
      || data["image/svg+xml"]
      || data["image/png"]
      || data["image/jpeg"]
    ) {
      return true;
    }
    if (Object.keys(data).some((key) => String(key || "").toLowerCase().startsWith("video/"))) {
      return true;
    }
    if (readMimeAsText(data["text/plain"] || "").length >= 6000) {
      return true;
    }
  }

  return false;
}

function shouldDeferCellOutput(cell) {
  if (!state.largeNotebookMode) return false;
  const key = String(cell?.id || "");
  if (!key) return false;
  if (state.outputExpandedCellIds.has(key)) return false;
  if (statusToClass(state.cellStatus[key]) === "running") return false;
  return isHeavyOutputCell(cell);
}

function normalizeBase64Text(value) {
  return String(value || "").replace(/\s+/g, "");
}

function estimateBase64Bytes(value) {
  const base64 = normalizeBase64Text(value);
  if (!base64) return 0;
  let padding = 0;
  if (base64.endsWith("==")) {
    padding = 2;
  } else if (base64.endsWith("=")) {
    padding = 1;
  }
  return Math.max(0, Math.floor((base64.length * 3) / 4) - padding);
}

function estimateSerializedBytes(value) {
  if (value == null) return 0;
  try {
    if (typeof value === "string") {
      return new Blob([value]).size;
    }
    return new Blob([JSON.stringify(value)]).size;
  } catch (_err) {
    return String(value).length;
  }
}

function formatMediaBytes(bytes) {
  return formatBytes(Math.max(0, Number(bytes || 0)));
}

function isOverMediaLazyThreshold(bytes) {
  return Number(bytes || 0) > MEDIA_LAZY_THRESHOLD_BYTES;
}

function yieldToMainThread() {
  return new Promise((resolve) => {
    if (typeof requestAnimationFrame === "function") {
      requestAnimationFrame(() => resolve());
    } else {
      setTimeout(resolve, 0);
    }
  });
}

async function decodeBase64ToBlobChunked(base64Value, mimeType, options = {}) {
  const base64 = normalizeBase64Text(base64Value);
  const mime = String(mimeType || "application/octet-stream");
  const chunkChars = Math.max(64 * 1024, Number(options.chunkChars || 256 * 1024));

  if (!base64) {
    return new Blob([], { type: mime });
  }

  const chunks = [];
  let sliceCount = 0;

  for (let offset = 0; offset < base64.length; offset += chunkChars) {
    const slice = base64.slice(offset, offset + chunkChars);
    const bin = atob(slice);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i += 1) {
      bytes[i] = bin.charCodeAt(i);
    }
    chunks.push(bytes);
    sliceCount += 1;

    if ((sliceCount % 2) === 0) {
      await yieldToMainThread();
    }
  }

  return new Blob(chunks, { type: mime });
}

function registerMediaObjectUrl(url) {
  const value = String(url || "");
  if (!value) return "";
  state.mediaObjectUrls.add(value);
  return value;
}

function createMediaPayloadToken() {
  state.mediaPayloadSeq = Number(state.mediaPayloadSeq || 0) + 1;
  return `media-${Date.now()}-${state.mediaPayloadSeq}`;
}

function cacheMediaPayload(mime, base64Value, sizeBytes) {
  const token = createMediaPayloadToken();
  state.mediaPayloadCache.set(token, {
    mime: String(mime || "application/octet-stream"),
    base64: normalizeBase64Text(base64Value),
    sizeBytes: Math.max(0, Number(sizeBytes || 0)),
  });
  return token;
}

function getCachedMediaPayload(token) {
  const key = String(token || "");
  if (!key) return null;
  const item = state.mediaPayloadCache.get(key);
  return item && typeof item === "object" ? item : null;
}

async function resolveMediaBase64FromOutput(outputItem, mimeHint = "") {
  const out = outputItem && typeof outputItem === "object" ? outputItem : {};
  const data = out.data && typeof out.data === "object" ? out.data : {};
  const meta = out.metadata && typeof out.metadata === "object" ? out.metadata : {};
  const mime = String(mimeHint || meta.neko_media_mime || "").toLowerCase();
  const token = String(meta.neko_media_token || "");

  if (token) {
    const cached = getCachedMediaPayload(token);
    if (cached && cached.base64) {
      return cached.base64;
    }
  }

  if (mime && typeof data[mime] === "string") {
    return normalizeBase64Text(data[mime]);
  }

  const candidateKey = Object.keys(data).find((key) => String(key || "").toLowerCase() === mime);
  if (candidateKey && typeof data[candidateKey] === "string") {
    return normalizeBase64Text(data[candidateKey]);
  }

  const assetRelPath = String(meta.neko_media_asset_path || "").trim();
  const notebookPathForAsset = String(meta.neko_notebook_path || state.currentPath || "");
  if (assetRelPath && notebookPathForAsset) {
    try {
      const resp = await callEntry(
        "load_notebook_media_asset_payload",
        {
          notebook_path: notebookPathForAsset,
          asset_rel_path: assetRelPath,
        },
        applyLargeNotebookAwareOptions({
          maxWaitMs: 300000,
          pollIntervalMs: 260,
          disableWorker: true,
        })
      );

      if (resp?.success && resp?.data && typeof resp.data === "object") {
        const mediaMime = String(resp.data.media_mime || mime || meta.neko_media_mime || "").toLowerCase();
        const raw = normalizeBase64Text(resp.data.base64 || "");
        if (mediaMime && raw) {
          const sizeBytes = Math.max(0, Number(resp.data.size_bytes || estimateBase64Bytes(raw)));
          const newToken = cacheMediaPayload(mediaMime, raw, sizeBytes);
          out.metadata = {
            ...(meta || {}),
            neko_media_token: newToken,
            neko_media_mime: mediaMime,
            neko_media_size_bytes: sizeBytes,
          };
          return raw;
        }
      }
    } catch (_err) {
      // Fall through to other lazy loading branches.
    }
  }

  const backendRef = meta.neko_media_backend_ref && typeof meta.neko_media_backend_ref === "object"
    ? meta.neko_media_backend_ref
    : null;
  const refPath = String(backendRef?.notebook_path || state.currentPath || "");
  const refCellIndex = Number(backendRef?.cell_index);
  if (refPath && Number.isInteger(refCellIndex) && refCellIndex >= 0) {
    try {
      const resp = await callEntry(
        "load_notebook_media_payload",
        {
          notebook_path: refPath,
          cell_index: refCellIndex,
          media_mime: mime || String(meta.neko_media_mime || ""),
        },
        applyLargeNotebookAwareOptions({
          maxWaitMs: 300000,
          pollIntervalMs: 260,
          disableWorker: true,
        })
      );

      if (resp?.success && resp?.data && typeof resp.data === "object") {
        const mediaMime = String(resp.data.media_mime || mime || "").toLowerCase();
        const raw = normalizeBase64Text(resp.data.base64 || "");
        if (mediaMime && raw) {
          const sizeBytes = Math.max(0, Number(resp.data.size_bytes || estimateBase64Bytes(raw)));
          const newToken = cacheMediaPayload(mediaMime, raw, sizeBytes);
          out.metadata = {
            ...(meta || {}),
            neko_media_token: newToken,
            neko_media_mime: mediaMime,
            neko_media_size_bytes: sizeBytes,
          };
          if (out.data && typeof out.data === "object") {
            delete out.data[mediaMime];
          }
          return raw;
        }
      }
    } catch (_err) {
      // Keep fallback behavior when backend lazy payload loading fails.
    }
  }

  return "";
}

function outputHasEmbeddedVideo(outputItem) {
  const out = outputItem && typeof outputItem === "object" ? outputItem : {};
  const data = out.data && typeof out.data === "object" ? out.data : {};
  const meta = out.metadata && typeof out.metadata === "object" ? out.metadata : {};
  if (String(meta.neko_media_kind || "").toLowerCase() === "video") return true;
  const mimeFromMeta = String(meta.neko_media_mime || "").toLowerCase();
  if (mimeFromMeta.startsWith("video/")) return true;
  return Object.keys(data).some((key) => String(key || "").toLowerCase().startsWith("video/"));
}

function cellHasEmbeddedVideo(cell) {
  if (Boolean(cell?.has_video_output)) {
    return true;
  }
  const outputs = Array.isArray(cell?.outputs) ? cell.outputs : [];
  for (const item of outputs) {
    if (outputHasEmbeddedVideo(item)) {
      return true;
    }
  }
  return false;
}

function compactOutputForClient(outputItem) {
  if (!outputItem || typeof outputItem !== "object") {
    return outputItem;
  }

  const cloned = {
    ...outputItem,
    data: outputItem.data && typeof outputItem.data === "object" ? { ...outputItem.data } : {},
    metadata: outputItem.metadata && typeof outputItem.metadata === "object" ? { ...outputItem.metadata } : {},
  };

  const meta = cloned.metadata;
  const existingToken = String(meta.neko_media_token || "");
  const existingMime = String(meta.neko_media_mime || "").toLowerCase();
  if (existingToken && existingMime && !cloned.data[existingMime]) {
    return cloned;
  }

  const videoMime = Object.keys(cloned.data).find((key) => String(key || "").toLowerCase().startsWith("video/"));
  if (!videoMime) {
    return cloned;
  }

  const raw = cloned.data[videoMime];
  if (typeof raw !== "string" || !raw) {
    return cloned;
  }

  const quickBytes = Math.floor((raw.length * 3) / 4);
  const sizeBytes = Math.max(Number(meta.neko_media_size_bytes || 0), quickBytes);
  if (sizeBytes <= MEDIA_LAZY_THRESHOLD_BYTES) {
    cloned.metadata.neko_media_size_bytes = sizeBytes;
    cloned.metadata.neko_media_mime = String(videoMime).toLowerCase();
    return cloned;
  }

  const token = cacheMediaPayload(videoMime, raw, sizeBytes);
  delete cloned.data[videoMime];
  cloned.metadata.neko_media_token = token;
  cloned.metadata.neko_media_mime = String(videoMime).toLowerCase();
  cloned.metadata.neko_media_size_bytes = sizeBytes;
  return cloned;
}

function revokeAllMediaObjectUrls() {
  for (const url of Array.from(state.mediaObjectUrls.values())) {
    try {
      URL.revokeObjectURL(url);
    } catch (_err) {
      // Ignore stale object URL revoke errors.
    }
  }
  state.mediaObjectUrls.clear();
}

function createLazyMediaPlaceholder(options = {}) {
  const wrap = document.createElement("div");
  wrap.className = `jp-media-lazy-card ${options.kind === "video" ? "is-video" : ""}`;

  const poster = document.createElement("div");
  poster.className = "jp-media-lazy-poster";
  poster.textContent = options.kind === "video" ? "视频内容" : "媒体内容";

  const hint = document.createElement("div");
  hint.className = "jp-media-lazy-hint";
  hint.textContent = String(options.hint || "媒体内容已延迟加载");

  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "jp-media-lazy-btn";
  btn.textContent = String(options.buttonText || "点击加载媒体内容");
  btn.addEventListener("click", async () => {
    if (btn.disabled) return;
    btn.disabled = true;
    const prev = btn.textContent;
    btn.textContent = "加载中...";
    try {
      if (typeof options.onLoad === "function") {
        await options.onLoad(wrap);
      }
    } catch (err) {
      showToast(`加载媒体失败: ${String(err)}`, "error");
      btn.disabled = false;
      btn.textContent = prev;
      return;
    }
  });

  wrap.appendChild(poster);
  wrap.appendChild(hint);
  wrap.appendChild(btn);
  return wrap;
}

function collapseAndUnloadInactiveLargeCells(nowMs) {
  if (!state.largeNotebookMode || !Array.isArray(state.notebook?.cells)) return false;
  const now = Number(nowMs || Date.now());
  let changed = false;

  for (const cell of state.notebook.cells) {
    const id = String(cell?.id || "");
    if (!id) continue;
    if (id === String(state.selectedCellId || "")) continue;
    if (statusToClass(state.cellStatus[id]) === "running") continue;
    if (updateTimers.has(id) || state.cellSyncPromises.has(id) || state.cellDetailHydrationPromises.has(id)) continue;

    const rendered = document.querySelector(`.jp-cell[data-cell-id="${id}"]`);
    if (rendered instanceof HTMLElement) continue;

    const lastActive = Number(state.cellLastActiveAt.get(id) || 0);
    if (lastActive > 0 && (now - lastActive) < NOTEBOOK_INACTIVE_UNLOAD_MS) continue;

    const sourceText = String(cell.source || "");
    const lineCount = Math.max(1, sourceText ? sourceText.split(/\r\n|\r|\n/).length : Number(cell.source_line_count || 1));
    const outputs = Array.isArray(cell.outputs) ? cell.outputs : [];

    if (!state.collapsedCellIds.has(id)) {
      state.collapsedCellIds.add(id);
      changed = true;
    }

    if (cell.details_loaded !== false || sourceText || outputs.length || String(cell.output_text || "")) {
      cell.source_line_count = lineCount;
      cell.output_count = Math.max(Number(cell.output_count || 0), outputs.length);
      cell.has_output = hasCellOutput(cell);
      cell.source = "";
      cell.outputs = [];
      cell.output_text = "";
      cell.details_loaded = false;
      cell.outline_only = true;
      changed = true;
    }

    state.outputExpandedCellIds.delete(id);
    state.virtualCellHeights.delete(id);
  }

  return changed;
}

async function decodeWorkerBinaryPayload(payload) {
  if (!payload || typeof payload !== "object") {
    throw new Error("invalid binary payload");
  }

  const encoding = String(payload.encoding || "");
  const raw = payload.payload;
  if (!(raw instanceof ArrayBuffer)) {
    throw new Error("binary payload missing ArrayBuffer");
  }

  let bytes = new Uint8Array(raw);
  if (encoding === "gzip-json") {
    if (typeof DecompressionStream === "undefined") {
      throw new Error("DecompressionStream unsupported");
    }
    const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
    const buf = await new Response(stream).arrayBuffer();
    bytes = new Uint8Array(buf);
  }

  const text = new TextDecoder("utf-8").decode(bytes);
  return JSON.parse(text);
}

function ensureNotebookWorker() {
  if (state.worker || typeof Worker === "undefined") return;
  try {
    const worker = new Worker("./notebook_worker.js");
    worker.addEventListener("message", (event) => {
      const payload = event?.data;
      if (!payload || typeof payload !== "object") return;
      const requestId = String(payload.requestId || "");
      if (!requestId) return;

      const pending = state.workerRequests.get(requestId);
      if (!pending) return;

      (async () => {
        if (payload.type === "result") {
          state.workerRequests.delete(requestId);
          pending.resolve(payload.result || { success: false, data: {}, error: "empty worker result" });
          return;
        }

        if (payload.type === "result-bin") {
          try {
            const result = await decodeWorkerBinaryPayload(payload);
            state.workerRequests.delete(requestId);
            pending.resolve(result || { success: false, data: {}, error: "empty binary worker result" });
          } catch (err) {
            state.workerRequests.delete(requestId);
            pending.reject(err instanceof Error ? err : new Error(String(err)));
          }
          return;
        }

        state.workerRequests.delete(requestId);
        pending.reject(new Error(String(payload.error || "worker request failed")));
      })();
    });

    worker.addEventListener("error", (event) => {
      state.workerReady = false;
      try {
        showToast(`后台线程异常: ${String(event?.message || "unknown error")}`, "error");
      } catch (_err) {
        // Ignore UI errors during worker exception handling.
      }
    });

    state.worker = worker;
    state.workerReady = true;
  } catch (_err) {
    state.worker = null;
    state.workerReady = false;
  }
}

function workerCallEntry(entryId, args = {}, options = {}) {
  ensureNotebookWorker();
  if (!state.worker || !state.workerReady) {
    throw new Error("worker unavailable");
  }

  const requestId = `wk-${Date.now()}-${state.workerSeq += 1}`;
  const token = localStorage.getItem("auth_token") || "";
  const priority = Number(options.priority ?? ENTRY_PRIORITY[String(entryId || "")] ?? 50);
  const pollIntervalMs = Math.max(80, Number(options.pollIntervalMs || DEFAULT_POLL_INTERVAL_MS));
  const maxWaitMs = Math.max(pollIntervalMs, Number(options.maxWaitMs || DEFAULT_MAX_WAIT_MS));

  return new Promise((resolve, reject) => {
    state.workerRequests.set(requestId, { resolve, reject, createdAt: Date.now() });
    state.worker.postMessage({
      type: "runEntry",
      requestId,
      payload: {
        pluginId: state.pluginId,
        entryId: String(entryId || ""),
        args,
        token,
        pollIntervalMs,
        maxWaitMs,
        priority,
      },
    });
  });
}

function workerRunTask(taskType, payload = {}, options = {}) {
  ensureNotebookWorker();
  if (!state.worker || !state.workerReady) {
    throw new Error("worker unavailable");
  }

  const requestId = `wkt-${Date.now()}-${state.workerSeq += 1}`;
  const token = localStorage.getItem("auth_token") || "";
  const pollIntervalMs = Math.max(80, Number(options.pollIntervalMs || DEFAULT_POLL_INTERVAL_MS));
  const maxWaitMs = Math.max(pollIntervalMs, Number(options.maxWaitMs || DEFAULT_MAX_WAIT_MS));

  return new Promise((resolve, reject) => {
    state.workerRequests.set(requestId, { resolve, reject, createdAt: Date.now() });
    state.worker.postMessage({
      type: "task",
      requestId,
      payload: {
        taskType: String(taskType || ""),
        ...(payload && typeof payload === "object" ? payload : {}),
        pluginId: state.pluginId,
        token,
        pollIntervalMs,
        maxWaitMs,
      },
    });
  });
}

function scheduleNotebookListRefresh(delayMs = 900) {
  const delay = Math.max(120, Number(delayMs || 900));
  if (notebookListRefreshTimer) {
    clearTimeout(notebookListRefreshTimer);
  }
  notebookListRefreshTimer = setTimeout(() => {
    notebookListRefreshTimer = null;
    refreshNotebookList().catch(() => {
      // Ignore refresh failures to avoid interrupting editing flow.
    });
  }, delay);
}

function collectClientGarbage() {
  const now = Date.now();
  const cellIds = new Set((state.notebook?.cells || []).map((cell) => String(cell.id || "")));
  const usedMediaTokens = new Set();
  for (const cell of (state.notebook?.cells || [])) {
    const outputs = Array.isArray(cell?.outputs) ? cell.outputs : [];
    for (const out of outputs) {
      if (!out || typeof out !== "object") continue;
      const meta = out.metadata && typeof out.metadata === "object" ? out.metadata : {};
      const token = String(meta.neko_media_token || "");
      if (token) {
        usedMediaTokens.add(token);
      }
    }
  }

  for (const [key, timer] of updateTimers.entries()) {
    if (!cellIds.has(String(key))) {
      clearTimeout(timer);
      updateTimers.delete(key);
    }
  }

  for (const key of Array.from(state.cellSyncPromises.keys())) {
    if (!cellIds.has(String(key))) {
      state.cellSyncPromises.delete(key);
    }
  }

  for (const key of Array.from(state.cellLastActiveAt.keys())) {
    if (!cellIds.has(String(key))) {
      state.cellLastActiveAt.delete(key);
    }
  }

  for (const key of Array.from(state.outputExpandedCellIds.values())) {
    if (!cellIds.has(String(key))) {
      state.outputExpandedCellIds.delete(String(key));
    }
  }

  if ((state.virtualCellHeights?.size || 0) > Math.max(120, cellIds.size * 2)) {
    for (const key of Array.from(state.virtualCellHeights.keys())) {
      if (!cellIds.has(String(key))) {
        state.virtualCellHeights.delete(key);
      }
    }
  }

  for (const [requestId, pending] of state.workerRequests.entries()) {
    if ((now - Number(pending?.createdAt || now)) > 180000) {
      try {
        pending.reject(new Error("worker request timed out"));
      } catch (_err) {
        // Ignore timeout rejection races.
      }
      state.workerRequests.delete(requestId);
    }
  }

  for (const token of Array.from(state.mediaPayloadCache.keys())) {
    if (!usedMediaTokens.has(String(token))) {
      state.mediaPayloadCache.delete(String(token));
    }
  }

  if (collapseAndUnloadInactiveLargeCells(now)) {
    renderCurrentNotebook(true);
  }
}

function clearRunQueueTimer() {
  if (runQueue.timer) {
    clearInterval(runQueue.timer);
    runQueue.timer = null;
  }
}

function ensureRunQueueTimer(pollIntervalMs) {
  if (runQueue.timer) return;
  const interval = Math.max(80, Number(pollIntervalMs || DEFAULT_POLL_INTERVAL_MS));
  runQueue.timer = setInterval(() => {
    drainRunQueue().catch(() => {
      // Ignore queue-level polling failures; each run has its own timeout.
    });
  }, interval);
}

function trackRun(runId, options = {}) {
  const pollIntervalMs = Math.max(80, Number(options.pollIntervalMs || DEFAULT_POLL_INTERVAL_MS));
  const maxWaitMs = Math.max(pollIntervalMs, Number(options.maxWaitMs || DEFAULT_MAX_WAIT_MS));

  return new Promise((resolve, reject) => {
    runQueue.pending.set(String(runId), {
      runId: String(runId),
      createdAt: Date.now(),
      lastPollAt: 0,
      pollIntervalMs,
      maxWaitMs,
      resolve,
      reject,
      failures: 0,
    });
    ensureRunQueueTimer(pollIntervalMs);
    void drainRunQueue();
  });
}

async function drainRunQueue() {
  if (runQueue.ticking) return;
  runQueue.ticking = true;
  try {
    const now = Date.now();
    const tasks = Array.from(runQueue.pending.values());
    if (!tasks.length) {
      clearRunQueueTimer();
      return;
    }

    await Promise.all(tasks.map(async (task) => {
      if (!task || !task.runId) return;
      if ((now - Number(task.createdAt || now)) > Number(task.maxWaitMs || DEFAULT_MAX_WAIT_MS)) {
        runQueue.pending.delete(task.runId);
        task.reject(new Error("等待运行结果超时"));
        return;
      }
      if ((now - Number(task.lastPollAt || 0)) < Number(task.pollIntervalMs || DEFAULT_POLL_INTERVAL_MS)) {
        return;
      }

      task.lastPollAt = now;
      try {
        const statusResp = await fetch(`${API_BASE}/runs/${task.runId}`, { headers: authHeaders() });
        if (!statusResp.ok) {
          task.failures = Number(task.failures || 0) + 1;
          if (task.failures >= 6) {
            runQueue.pending.delete(task.runId);
            task.reject(new Error(`获取运行状态失败: HTTP ${statusResp.status}`));
          }
          return;
        }

        const statusObj = await statusResp.json();
        if (statusObj.status !== "succeeded" && statusObj.status !== "failed") {
          return;
        }

        runQueue.pending.delete(task.runId);
        const result = await readRunExport(task.runId, statusObj.status);
        task.resolve(result);
      } catch (err) {
        task.failures = Number(task.failures || 0) + 1;
        if (task.failures >= 6) {
          runQueue.pending.delete(task.runId);
          task.reject(err instanceof Error ? err : new Error(String(err)));
        }
      }
    }));
  } finally {
    runQueue.ticking = false;
    if (!runQueue.pending.size) {
      clearRunQueueTimer();
    }
  }
}

const MENU_CONFIG = {
  文件: [
    { label: "新建笔记本", action: "file.new" },
    { label: "保存笔记本", action: "file.save" },
  ],
  编辑: [
    { label: "查找并替换", action: "edit.findReplace" },
    { label: "上移单元格", action: "edit.moveUp" },
    { label: "下移单元格", action: "edit.moveDown" },
    { label: "合并与下方单元格", action: "edit.mergeWithNext" },
    { label: "从光标位置拆分", action: "edit.splitAtCursor" },
    { label: "删除选中单元格", action: "edit.deleteSelected" },
  ],
  查看: [
    { label: "收起/展开左侧栏", action: "view.toggleSidebar" },
    { label: "折叠全部单元格", action: "view.collapseAll" },
    { label: "展开全部单元格", action: "view.expandAll" },
  ],
  运行: [
    { label: "运行选中单元格", action: "run.selected" },
    { label: "运行全部代码单元格", action: "run.all" },
    { label: "清空全部输出", action: "run.clearOutputs" },
  ],
  内核: [{ label: "重启内核", action: "kernel.restart" }],
  设置: [{ label: "切换移动端工具栏模式", action: "settings.mobileToolbar" }],
  帮助: [{ label: "显示快捷提示", action: "help.shortcuts" }],
};

const FLOATING_PREFS_KEY = "neko_notebook_floating_prefs";
const FLOATING_DEFAULT_PREFS = {
  compactButtons: false,
  dockRight: 14,
  dockX: null,
  dockY: null,
  dockCollapsed: false,
  autoCloseAfterAction: false,
  voiceAutoSend: false,
};

function authHeaders() {
  const token = localStorage.getItem("auth_token") || "";
  return {
    "Content-Type": "application/json",
    Authorization: token ? `Bearer ${token}` : "",
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function nowPerfMs() {
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return performance.now();
  }
  return Date.now();
}

function diffPerfMs(start, end) {
  const s = Number(start);
  const e = Number(end);
  if (!Number.isFinite(s) || !Number.isFinite(e)) return null;
  return Math.max(0, Math.round((e - s) * 1000) / 1000);
}

function toIntMs(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.max(0, Math.round(n));
}

function emitRunLatencyTrace(trace, backendPerf, options = {}) {
  const backend = backendPerf && typeof backendPerf === "object" ? backendPerf : {};
  const timeline = backend.timeline_ms && typeof backend.timeline_ms === "object" ? backend.timeline_ms : {};
  const serverStage = backend.server_stage_ms && typeof backend.server_stage_ms === "object" ? backend.server_stage_ms : {};
  const kernelStage = backend.kernel_stage_ms && typeof backend.kernel_stage_ms === "object" ? backend.kernel_stage_ms : {};

  const rows = [
    {
      step: "点击按钮→事件触发",
      ms: toIntMs(diffPerfMs(trace?.clickAt, trace?.eventTriggeredAt)),
    },
    {
      step: "事件触发→指令发送",
      ms: toIntMs(diffPerfMs(trace?.eventTriggeredAt, trace?.dispatchStartedAt)),
    },
    {
      step: "指令发送→内核接收",
      ms: toIntMs(timeline.dispatch_to_kernel_receive_ms),
    },
    {
      step: "内核接收→内核执行",
      ms: toIntMs(timeline.kernel_receive_to_kernel_execute_ms),
    },
    {
      step: "内核执行→结果返回",
      ms: toIntMs(timeline.kernel_execute_to_result_return_ms),
    },
    {
      step: "结果返回→前端渲染",
      ms: toIntMs(diffPerfMs(trace?.responseReceivedAt, trace?.renderCompletedAt)),
    },
  ];

  const totalMs = toIntMs(diffPerfMs(trace?.clickAt, trace?.renderCompletedAt));
  const label = String(options?.success) ? "SUCCESS" : "FAILED";
  const trigger = String(trace?.trigger || "unknown");
  const cellId = String(trace?.cellId || "");

  try {
    console.groupCollapsed(`[NEKO][RunTrace][${label}] cell=${cellId} trigger=${trigger} total=${totalMs ?? "?"}ms`);
    if (typeof console.table === "function") {
      console.table(rows);
    } else {
      console.log("run_trace_steps", rows);
    }
    console.log("run_trace_frontend", {
      trigger,
      cell_id: cellId,
      click_to_render_ms: totalMs,
      run_create_ms: toIntMs(diffPerfMs(trace?.dispatchStartedAt, trace?.runCreatedAt)),
      run_terminal_ms: toIntMs(diffPerfMs(trace?.runCreatedAt, trace?.runTerminalAt)),
      export_fetch_ms: toIntMs(diffPerfMs(trace?.exportRequestedAt, trace?.exportReceivedAt)),
    });
    console.log("run_trace_backend_server_stage_ms", serverStage);
    console.log("run_trace_backend_kernel_stage_ms", kernelStage);
    console.groupEnd();
  } catch (_err) {
    // Ignore console instrumentation errors to keep execution flow safe.
  }
}

function showToast(message, type = "info") {
  const toast = document.getElementById("toast");
  if (!toast) return;
  toast.textContent = String(message || "");
  toast.className = `jp-toast show ${type}`;
  if (toastTimer) {
    clearTimeout(toastTimer);
  }
  toastTimer = setTimeout(() => {
    toast.className = "jp-toast";
  }, 1600);
}

function loadFloatingPrefs() {
  try {
    const raw = localStorage.getItem(FLOATING_PREFS_KEY);
    if (!raw) {
      state.floatingPrefs = { ...FLOATING_DEFAULT_PREFS };
      return;
    }
    const parsed = JSON.parse(raw);
    state.floatingPrefs = {
      ...FLOATING_DEFAULT_PREFS,
      ...(parsed && typeof parsed === "object" ? parsed : {}),
    };
  } catch (_err) {
    state.floatingPrefs = { ...FLOATING_DEFAULT_PREFS };
  }
}

function persistFloatingPrefs() {
  try {
    localStorage.setItem(FLOATING_PREFS_KEY, JSON.stringify(state.floatingPrefs || FLOATING_DEFAULT_PREFS));
  } catch (_err) {
    // Ignore persistence errors.
  }
}

function getFloatingDock() {
  const dock = document.querySelector(".jp-floating-dock");
  return dock instanceof HTMLElement ? dock : null;
}

function clampDockPosition(x, y, dock) {
  const dockEl = dock || getFloatingDock();
  const margin = 8;
  const fallbackWidth = 52;
  const fallbackHeight = 52;
  const rect = dockEl ? dockEl.getBoundingClientRect() : null;
  const width = Math.max(fallbackWidth, Math.round(rect?.width || 0) || fallbackWidth);
  const height = Math.max(fallbackHeight, Math.round(rect?.height || 0) || fallbackHeight);

  const maxX = Math.max(margin, window.innerWidth - width - margin);
  const maxY = Math.max(margin, window.innerHeight - height - margin);

  return {
    x: Math.min(maxX, Math.max(margin, Math.round(Number(x) || 0))),
    y: Math.min(maxY, Math.max(margin, Math.round(Number(y) || 0))),
  };
}

function updateFloatingDockToggleButton() {
  const toggleBtn = document.getElementById("floating-dock-toggle");
  if (!(toggleBtn instanceof HTMLButtonElement)) return;
  const collapsed = Boolean(state.floatingPrefs?.dockCollapsed);
  toggleBtn.title = collapsed ? "展开快捷栏" : "折叠快捷栏";
  toggleBtn.setAttribute("aria-label", collapsed ? "展开快捷栏" : "折叠快捷栏");
}

function applyFloatingDockPosition() {
  const dock = getFloatingDock();
  if (!dock) return;

  if (window.innerWidth <= 900) {
    dock.style.left = "";
    dock.style.top = "";
    dock.style.right = "";
    dock.style.bottom = "";
    dock.style.transform = "";
    return;
  }

  const prefs = state.floatingPrefs || FLOATING_DEFAULT_PREFS;
  const right = Math.max(8, Math.min(42, Number(prefs.dockRight) || FLOATING_DEFAULT_PREFS.dockRight));
  const currentRect = dock.getBoundingClientRect();
  const defaultX = Math.max(8, window.innerWidth - Math.max(52, Math.round(currentRect.width || 52)) - right);
  const defaultY = Math.max(8, Math.round((window.innerHeight - Math.max(52, Math.round(currentRect.height || 52))) / 2));

  let x = Number(prefs.dockX);
  let y = Number(prefs.dockY);
  if (!Number.isFinite(x) || !Number.isFinite(y)) {
    x = defaultX;
    y = defaultY;
    prefs.dockX = x;
    prefs.dockY = y;
  }

  const clamped = clampDockPosition(x, y, dock);
  prefs.dockX = clamped.x;
  prefs.dockY = clamped.y;
  dock.style.left = `${clamped.x}px`;
  dock.style.top = `${clamped.y}px`;
  dock.style.right = "auto";
  dock.style.bottom = "auto";
  dock.style.transform = "none";
}

function setFloatingDockCollapsed(collapsed, persist = true) {
  const dock = getFloatingDock();
  state.floatingPrefs = state.floatingPrefs || { ...FLOATING_DEFAULT_PREFS };
  state.floatingPrefs.dockCollapsed = Boolean(collapsed);
  if (dock) {
    dock.classList.toggle("jp-dock-collapsed", Boolean(collapsed));
  }
  if (collapsed) {
    closeFloatingPanel();
  }
  updateFloatingDockToggleButton();
  applyFloatingDockPosition();
  if (persist) {
    persistFloatingPrefs();
  }
}

function applyFloatingPrefs() {
  const prefs = state.floatingPrefs || FLOATING_DEFAULT_PREFS;
  const right = Math.max(8, Math.min(42, Number(prefs.dockRight) || FLOATING_DEFAULT_PREFS.dockRight));
  document.documentElement.style.setProperty("--jp-floating-dock-right", `${right}px`);
  document.body.classList.toggle("jp-floating-compact", Boolean(prefs.compactButtons));
  setFloatingDockCollapsed(Boolean(prefs.dockCollapsed), false);
  applyFloatingDockPosition();
}

function setFloatingButtonActive(action) {
  document.querySelectorAll(".jp-float-btn[data-float-action]").forEach((btn) => {
    const key = String(btn.getAttribute("data-float-action") || "");
    btn.classList.toggle("active", key === action);
  });
}

function closeFloatingPanel() {
  const panel = document.getElementById("floating-panel");
  const body = document.getElementById("floating-panel-body");
  if (!panel || !body) return;
  panel.classList.add("hidden");
  body.innerHTML = "";
  state.floatingPanelAction = "";
  setFloatingButtonActive("");
}

function renderFloatingActionButtons(items) {
  return (items || [])
    .map((item) => `<button class="jp-float-action" data-float-command="${escapeHtml(item.command)}">${escapeHtml(item.label)}</button>`)
    .join("");
}

function renderFloatingSettingsPanel() {
  const prefs = state.floatingPrefs || FLOATING_DEFAULT_PREFS;
  return `
    <div class="jp-float-section">
      <p class="jp-float-section-title">全局设置</p>
      <label class="jp-float-form-row">
        <span>紧凑悬浮按钮</span>
        <input id="float-pref-compact" type="checkbox" ${prefs.compactButtons ? "checked" : ""} />
      </label>
      <label class="jp-float-form-row">
        <span>操作后自动收起面板</span>
        <input id="float-pref-auto-close" type="checkbox" ${prefs.autoCloseAfterAction ? "checked" : ""} />
      </label>
      <label class="jp-float-form-row">
        <span>语音后自动发送</span>
        <input id="float-pref-voice-auto" type="checkbox" ${prefs.voiceAutoSend ? "checked" : ""} />
      </label>
      <label class="jp-float-form-row">
        <span>悬浮栏右侧边距</span>
        <input id="float-pref-right" type="range" min="8" max="42" step="1" value="${Math.max(8, Math.min(42, Number(prefs.dockRight) || 14))}" />
      </label>
      <div id="float-pref-right-value" class="jp-float-note">当前边距: ${Math.max(8, Math.min(42, Number(prefs.dockRight) || 14))} px</div>
    </div>
    <div class="jp-float-note">设置已自动保存，页面缩放和窗口尺寸变化时会保持悬浮栏位置稳定。</div>
  `;
}

function renderFloatingToolsPanel() {
  const commonTools = [
    { label: "新建笔记本", command: "tool.newNotebook" },
    { label: "保存笔记本", command: "tool.save" },
    { label: "新增代码单元格", command: "tool.addCode" },
    { label: "新增标记单元格", command: "tool.addMarkdown" },
    { label: "新增视图单元格", command: "tool.addView" },
  ];
  const advancedTools = [
    { label: "查找并替换", command: "tool.findReplace" },
    { label: "清空全部输出", command: "tool.clearOutputs" },
    { label: "折叠全部单元格", command: "tool.collapseAll" },
    { label: "展开全部单元格", command: "tool.expandAll" },
  ];

  return `
    <div class="jp-float-section">
      <p class="jp-float-section-title">COMMON TOOLS</p>
      <div class="jp-float-grid">${renderFloatingActionButtons(commonTools)}</div>
    </div>
    <div class="jp-float-section">
      <p class="jp-float-section-title">ADVANCED TOOLS</p>
      <div class="jp-float-grid">${renderFloatingActionButtons(advancedTools)}</div>
    </div>
  `;
}

function renderFloatingCapturePanel() {
  const recording = Boolean(state.screenRecorder && state.screenRecorder.state !== "inactive");
  return `
    <div class="jp-float-section">
      <p class="jp-float-section-title">截图 / 录屏</p>
      <div class="jp-capture-status ${recording ? "recording" : ""}">${recording ? "正在录屏中，可再次点击结束" : "点击按钮后选择当前窗口即可开始截图或录屏"}</div>
      <div class="jp-float-grid" style="margin-top: 8px;">
        <button class="jp-float-action" data-float-command="capture.screenshot">截图</button>
        <button class="jp-float-action" data-float-command="capture.record">${recording ? "停止录屏" : "开始录屏"}</button>
      </div>
    </div>
    <div class="jp-float-note">截图会导出 PNG 文件，录屏会导出 WebM 文件，不会影响单元格执行状态。</div>
  `;
}

function renderFloatingAiPanel() {
  const logs = state.floatingAiHistory.length
    ? state.floatingAiHistory
      .slice(-12)
      .map((item) => `<div class="jp-ai-item ${escapeHtml(item.role || "user")}">${escapeHtml(item.content || "")}</div>`)
      .join("")
    : '<div class="jp-float-note">输入文字，或点击“开始听写”进行语音输入。</div>';

  return `
    <div class="jp-float-section">
      <p class="jp-float-section-title">AI 对话 / 语音交互</p>
      <div class="jp-ai-log">${logs}</div>
      <textarea id="floating-ai-input" class="jp-ai-input" placeholder="输入要发送到 AI 对话面板的内容...">${escapeHtml(state.floatingAiDraft || "")}</textarea>
      <div class="jp-ai-actions">
        <button class="jp-float-action" data-float-command="ai.send">发送</button>
        <button class="jp-float-action" data-float-command="ai.voice">${state.isSpeechListening ? "停止听写" : "开始听写"}</button>
        <button class="jp-float-action" data-float-command="ai.clear">清空</button>
      </div>
    </div>
  `;
}

function renderFloatingRunPanel() {
  const runActions = [
    { label: "运行选中", command: "run.selected" },
    { label: "运行全部", command: "run.all" },
    { label: "重启内核", command: "run.restartKernel" },
    { label: "清空输出", command: "tool.clearOutputs" },
  ];
  return `
    <div class="jp-float-section">
      <p class="jp-float-section-title">快捷运行控制</p>
      <div class="jp-float-grid">${renderFloatingActionButtons(runActions)}</div>
    </div>
  `;
}

function renderFloatingPanelByAction(action) {
  const panel = document.getElementById("floating-panel");
  const title = document.getElementById("floating-panel-title");
  const body = document.getElementById("floating-panel-body");
  if (!panel || !title || !body) return;

  if (action === "settings") {
    title.textContent = "插件全局设置";
    body.innerHTML = renderFloatingSettingsPanel();
  } else if (action === "tools") {
    title.textContent = "快捷工具菜单";
    body.innerHTML = renderFloatingToolsPanel();
  } else if (action === "capture") {
    title.textContent = "截图与录屏";
    body.innerHTML = renderFloatingCapturePanel();
  } else if (action === "ai") {
    title.textContent = "AI 对话与语音";
    body.innerHTML = renderFloatingAiPanel();
  } else if (action === "run") {
    title.textContent = "运行控制";
    body.innerHTML = renderFloatingRunPanel();
  } else {
    title.textContent = "快捷面板";
    body.innerHTML = "";
  }

  bindFloatingPanelActions(action);
}

function refreshFloatingPanel(action) {
  const panel = document.getElementById("floating-panel");
  if (!panel || panel.classList.contains("hidden")) return;
  if (state.floatingPanelAction !== action) return;
  renderFloatingPanelByAction(action);
}

function openFloatingPanel(action) {
  const panel = document.getElementById("floating-panel");
  if (!panel) return;

  if (state.floatingPanelAction === action && !panel.classList.contains("hidden")) {
    closeFloatingPanel();
    return;
  }

  state.floatingPanelAction = action;
  setFloatingButtonActive(action);
  renderFloatingPanelByAction(action);
  panel.classList.remove("hidden");
}

function downloadBlob(blob, fileName) {
  if (!blob) return;
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = fileName;
  anchor.rel = "noopener";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1200);
}

async function captureScreenShot() {
  if (!navigator.mediaDevices || typeof navigator.mediaDevices.getDisplayMedia !== "function") {
    showToast("当前浏览器不支持截图 API", "error");
    return;
  }

  let stream = null;
  try {
    stream = await navigator.mediaDevices.getDisplayMedia({
      video: { frameRate: 30 },
      audio: false,
    });
    const track = stream.getVideoTracks()[0];
    if (!track) {
      showToast("未获取到可截图的视频轨道", "error");
      return;
    }

    const video = document.createElement("video");
    video.srcObject = stream;
    video.muted = true;
    video.playsInline = true;
    await video.play();
    await sleep(120);

    const width = Math.max(1, video.videoWidth || window.innerWidth);
    const height = Math.max(1, video.videoHeight || window.innerHeight);
    const canvas = document.createElement("canvas");
    canvas.width = width;
    canvas.height = height;

    const ctx = canvas.getContext("2d");
    if (!ctx) {
      showToast("截图上下文创建失败", "error");
      return;
    }
    ctx.drawImage(video, 0, 0, width, height);

    const blob = await new Promise((resolve) => canvas.toBlob(resolve, "image/png", 0.98));
    if (!blob) {
      showToast("截图导出失败", "error");
      return;
    }
    downloadBlob(blob, `notebook-screenshot-${Date.now()}.png`);
    showToast("截图已下载", "success");
  } catch (err) {
    showToast(`截图取消或失败: ${String(err)}`, "info");
  } finally {
    if (stream) {
      stream.getTracks().forEach((track) => track.stop());
    }
  }
}

function stopRecordingStream() {
  if (state.recordingStream) {
    state.recordingStream.getTracks().forEach((track) => track.stop());
    state.recordingStream = null;
  }
}

function syncCapturePanelIfOpen() {
  refreshFloatingPanel("capture");
}

async function startScreenRecording() {
  if (!navigator.mediaDevices || typeof navigator.mediaDevices.getDisplayMedia !== "function") {
    showToast("当前浏览器不支持录屏 API", "error");
    return;
  }
  if (typeof MediaRecorder === "undefined") {
    showToast("当前浏览器不支持 MediaRecorder", "error");
    return;
  }

  if (state.screenRecorder && state.screenRecorder.state !== "inactive") {
    showToast("录屏已在进行中", "info");
    return;
  }

  try {
    const stream = await navigator.mediaDevices.getDisplayMedia({
      video: { frameRate: 30 },
      audio: true,
    });
    const supportsVp9 = typeof MediaRecorder !== "undefined" && MediaRecorder.isTypeSupported("video/webm;codecs=vp9,opus");
    const recorder = new MediaRecorder(stream, supportsVp9 ? { mimeType: "video/webm;codecs=vp9,opus" } : undefined);

    state.recordedChunks = [];
    state.screenRecorder = recorder;
    state.recordingStream = stream;

    recorder.addEventListener("dataavailable", (event) => {
      if (event.data && event.data.size > 0) {
        state.recordedChunks.push(event.data);
      }
    });

    recorder.addEventListener("stop", () => {
      const chunks = Array.isArray(state.recordedChunks) ? state.recordedChunks : [];
      if (chunks.length) {
        const blob = new Blob(chunks, { type: "video/webm" });
        downloadBlob(blob, `notebook-record-${Date.now()}.webm`);
        showToast("录屏已保存", "success");
      }
      state.recordedChunks = [];
      state.screenRecorder = null;
      stopRecordingStream();
      syncCapturePanelIfOpen();
    });

    const videoTrack = stream.getVideoTracks()[0];
    if (videoTrack) {
      videoTrack.addEventListener("ended", () => {
        if (state.screenRecorder && state.screenRecorder.state !== "inactive") {
          state.screenRecorder.stop();
        }
      });
    }

    recorder.start(1000);
    showToast("录屏开始", "success");
    syncCapturePanelIfOpen();
  } catch (err) {
    showToast(`录屏取消或失败: ${String(err)}`, "info");
    state.screenRecorder = null;
    state.recordedChunks = [];
    stopRecordingStream();
    syncCapturePanelIfOpen();
  }
}

function stopScreenRecording() {
  if (!state.screenRecorder || state.screenRecorder.state === "inactive") {
    showToast("当前没有进行中的录屏", "info");
    stopRecordingStream();
    return;
  }
  state.screenRecorder.stop();
  showToast("录屏停止中...", "info");
}

function getSpeechRecognitionCtor() {
  return window.SpeechRecognition || window.webkitSpeechRecognition || null;
}

function syncAiPanelIfOpen() {
  refreshFloatingPanel("ai");
}

function getFloatingAiInput() {
  const input = document.getElementById("floating-ai-input");
  return input instanceof HTMLTextAreaElement ? input : null;
}

function stopVoiceInput() {
  if (state.speechRecognition && state.isSpeechListening) {
    state.speechRecognition.stop();
  }
  state.isSpeechListening = false;
  syncAiPanelIfOpen();
}

function startVoiceInput() {
  const Ctor = getSpeechRecognitionCtor();
  if (!Ctor) {
    showToast("当前浏览器不支持语音识别", "error");
    return;
  }

  const input = getFloatingAiInput();
  if (!input) {
    showToast("请先打开 AI 面板", "info");
    return;
  }

  if (!state.speechRecognition) {
    const recognition = new Ctor();
    recognition.lang = "zh-CN";
    recognition.continuous = true;
    recognition.interimResults = true;

    recognition.onresult = (event) => {
      let transcript = "";
      for (let i = event.resultIndex; i < event.results.length; i += 1) {
        transcript += event.results[i][0]?.transcript || "";
      }
      const target = getFloatingAiInput();
      if (target) {
        const nextValue = `${String(target.value || "")} ${transcript}`.trim();
        target.value = nextValue;
        state.floatingAiDraft = nextValue;
      }
    };

    recognition.onerror = () => {
      state.isSpeechListening = false;
      syncAiPanelIfOpen();
    };

    recognition.onend = () => {
      state.isSpeechListening = false;
      syncAiPanelIfOpen();
      if (state.floatingPrefs?.voiceAutoSend) {
        void sendFloatingAiMessage();
      }
    };

    state.speechRecognition = recognition;
  }

  if (state.isSpeechListening) {
    stopVoiceInput();
    return;
  }

  state.isSpeechListening = true;
  state.speechRecognition.start();
  syncAiPanelIfOpen();
  showToast("语音听写已开启", "success");
}

async function sendFloatingAiMessage() {
  const input = getFloatingAiInput();
  const text = String((input ? input.value : state.floatingAiDraft) || "").trim();
  if (!text) {
    showToast("请输入或听写内容后再发送", "info");
    return;
  }

  try {
    const messageId = `floating-ai-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
    state.floatingAiHistory.push({ role: "user", content: text });
    state.floatingAiHistory.push({ role: "assistant", content: "消息发送中..." });
    syncAiPanelIfOpen();

    const resp = await callEntry("send_ai_message", {
      text,
      user_identity: "用户",
      message_id: messageId,
    });

    if (!resp.success) {
      state.floatingAiHistory[state.floatingAiHistory.length - 1] = {
        role: "assistant",
        content: `发送失败: ${String(resp.error || "消息通道异常，请稍后重试")}`,
      };
      state.floatingAiDraft = text;
      if (input) input.value = text;
      syncAiPanelIfOpen();
      showToast("发送失败，请重试", "error");
      return;
    }

    state.floatingAiHistory[state.floatingAiHistory.length - 1] = {
      role: "assistant",
      content: "已推送到主对话模型，等待回复。",
    };
    state.floatingAiDraft = "";
    if (input) input.value = "";
    syncAiPanelIfOpen();
    showToast("已发送到主对话模型", "success");
  } catch (err) {
    if (state.floatingAiHistory.length && state.floatingAiHistory[state.floatingAiHistory.length - 1]?.role === "assistant") {
      state.floatingAiHistory[state.floatingAiHistory.length - 1] = {
        role: "assistant",
        content: `发送失败: ${String(err)}`,
      };
    }
    state.floatingAiDraft = text;
    if (input) input.value = text;
    syncAiPanelIfOpen();
    showToast("发送失败，请重试", "error");
  }
}

async function executeFloatingCommand(command) {
  switch (command) {
    case "tool.newNotebook":
      await createNotebook();
      break;
    case "tool.save":
      await saveCurrentNotebook();
      break;
    case "tool.addCode":
      await addCell("code");
      break;
    case "tool.addMarkdown":
      await addCell("markdown");
      break;
    case "tool.addView":
      await addCell("view");
      break;
    case "tool.findReplace":
      await findAndReplaceInNotebook();
      break;
    case "tool.clearOutputs":
      await clearAllOutputs();
      break;
    case "tool.collapseAll":
      collapseAllCells();
      break;
    case "tool.expandAll":
      expandAllCells();
      break;
    case "run.selected":
      await runSelectedCell();
      break;
    case "run.all":
      await runAllCodeCells();
      break;
    case "run.restartKernel":
      await restartKernel();
      break;
    case "capture.screenshot":
      await captureScreenShot();
      break;
    case "capture.record":
      if (state.screenRecorder && state.screenRecorder.state !== "inactive") {
        stopScreenRecording();
      } else {
        await startScreenRecording();
      }
      break;
    case "ai.send":
      await sendFloatingAiMessage();
      break;
    case "ai.voice":
      startVoiceInput();
      break;
    case "ai.clear":
      state.floatingAiHistory = [];
      state.floatingAiDraft = "";
      syncAiPanelIfOpen();
      break;
    default:
      showToast("该快捷操作暂未实现", "info");
      break;
  }

  if (state.floatingPrefs?.autoCloseAfterAction && !String(command).startsWith("capture.") && !String(command).startsWith("ai.")) {
    closeFloatingPanel();
  }
}

function bindFloatingPanelActions(action) {
  const body = document.getElementById("floating-panel-body");
  if (!body) return;

  if (action === "settings") {
    const compact = document.getElementById("float-pref-compact");
    const autoClose = document.getElementById("float-pref-auto-close");
    const voiceAuto = document.getElementById("float-pref-voice-auto");
    const dockRight = document.getElementById("float-pref-right");
    const dockRightValue = document.getElementById("float-pref-right-value");

    compact?.addEventListener("change", () => {
      state.floatingPrefs.compactButtons = Boolean(compact.checked);
      applyFloatingPrefs();
      persistFloatingPrefs();
    });
    autoClose?.addEventListener("change", () => {
      state.floatingPrefs.autoCloseAfterAction = Boolean(autoClose.checked);
      persistFloatingPrefs();
    });
    voiceAuto?.addEventListener("change", () => {
      state.floatingPrefs.voiceAutoSend = Boolean(voiceAuto.checked);
      persistFloatingPrefs();
    });
    dockRight?.addEventListener("input", () => {
      const value = Math.max(8, Math.min(42, Number(dockRight.value) || 14));
      state.floatingPrefs.dockRight = value;
      if (dockRightValue) dockRightValue.textContent = `当前边距: ${value} px`;
      applyFloatingPrefs();
      persistFloatingPrefs();
    });
    return;
  }

  if (action === "ai") {
    const input = getFloatingAiInput();
    if (input) {
      input.addEventListener("input", () => {
        state.floatingAiDraft = input.value;
      });
    }
  }

  body.querySelectorAll("[data-float-command]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const cmd = String(btn.getAttribute("data-float-command") || "");
      await executeFloatingCommand(cmd);
    });
  });
}

function bindFloatingDockEvents() {
  document.querySelectorAll(".jp-float-btn[data-float-action]").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      event.stopPropagation();
      const action = String(btn.getAttribute("data-float-action") || "");
      openFloatingPanel(action);
    });
  });

  const dock = getFloatingDock();
  const toggleBtn = document.getElementById("floating-dock-toggle");
  if (dock && toggleBtn) {
    let drag = null;
    let suppressClickUntil = 0;

    toggleBtn.addEventListener("pointerdown", (event) => {
      if (window.innerWidth <= 900) return;
      event.stopPropagation();

      const rect = dock.getBoundingClientRect();
      drag = {
        pointerId: event.pointerId,
        offsetX: event.clientX - rect.left,
        offsetY: event.clientY - rect.top,
        moved: false,
      };

      dock.classList.add("is-dragging");
      try {
        toggleBtn.setPointerCapture(event.pointerId);
      } catch (_err) {
        // Ignore browsers that do not support pointer capture.
      }
    });

    const finishDrag = () => {
      if (!drag) return;
      try {
        toggleBtn.releasePointerCapture(drag.pointerId);
      } catch (_err) {
        // Ignore release failures.
      }
      if (drag.moved) {
        suppressClickUntil = Date.now() + 180;
        persistFloatingPrefs();
      }
      dock.classList.remove("is-dragging");
      drag = null;
    };

    toggleBtn.addEventListener("pointermove", (event) => {
      if (!drag || drag.pointerId !== event.pointerId) return;
      event.stopPropagation();

      const nextX = event.clientX - drag.offsetX;
      const nextY = event.clientY - drag.offsetY;
      const clamped = clampDockPosition(nextX, nextY, dock);

      if (Math.abs(nextX - (state.floatingPrefs?.dockX || 0)) > 2 || Math.abs(nextY - (state.floatingPrefs?.dockY || 0)) > 2) {
        drag.moved = true;
      }

      state.floatingPrefs.dockX = clamped.x;
      state.floatingPrefs.dockY = clamped.y;
      applyFloatingDockPosition();
    });

    toggleBtn.addEventListener("pointerup", (event) => {
      if (!drag || drag.pointerId !== event.pointerId) return;
      event.stopPropagation();
      finishDrag();
    });

    toggleBtn.addEventListener("pointercancel", () => {
      finishDrag();
    });

    toggleBtn.addEventListener("click", (event) => {
      event.stopPropagation();
      if (Date.now() < suppressClickUntil) return;
      setFloatingDockCollapsed(!Boolean(state.floatingPrefs?.dockCollapsed), true);
    });
  }

  document.getElementById("floating-panel-close")?.addEventListener("click", (event) => {
    event.stopPropagation();
    closeFloatingPanel();
  });
}

function closeMenuPanel() {
  const panel = document.getElementById("menu-panel");
  if (!panel) return;
  panel.classList.add("hidden");
  panel.innerHTML = "";
}

function openMenuPanel(menuName, anchorEl) {
  const panel = document.getElementById("menu-panel");
  if (!panel || !anchorEl) return;

  const items = MENU_CONFIG[menuName] || [];
  if (!items.length) {
    showToast(`${menuName} 菜单暂无可用操作`, "info");
    return;
  }

  panel.innerHTML = "";
  const title = document.createElement("div");
  title.className = "jp-menu-panel-title";
  title.textContent = menuName;
  panel.appendChild(title);

  items.forEach((item) => {
    const btn = document.createElement("button");
    btn.className = "jp-menu-action";
    btn.textContent = item.label;
    btn.addEventListener("click", async (event) => {
      event.stopPropagation();
      closeMenuPanel();
      await executeMenuAction(item.action);
    });
    panel.appendChild(btn);
  });

  const rect = anchorEl.getBoundingClientRect();
  panel.style.top = `${Math.round(rect.bottom + 6)}px`;
  panel.style.left = `${Math.round(rect.left)}px`;
  panel.classList.remove("hidden");
}

function toggleMobileToolbar(force) {
  const toolbar = document.querySelector(".jp-toolbar");
  const toggleBtn = document.getElementById("mobile-toolbar-toggle");
  if (!toolbar || !toggleBtn) return;

  if (typeof force === "boolean") {
    state.mobileToolbarExpanded = force;
  } else {
    state.mobileToolbarExpanded = !state.mobileToolbarExpanded;
  }

  toolbar.classList.toggle("mobile-expanded", state.mobileToolbarExpanded);
  toggleBtn.textContent = state.mobileToolbarExpanded ? "收起工具栏" : "展开工具栏";
}

function bindEvents() {
  bindFloatingDockEvents();

  document.getElementById("refresh-list-btn")?.addEventListener("click", refreshNotebookList);
  document.getElementById("new-notebook-btn")?.addEventListener("click", createNotebook);
  document.getElementById("save-btn")?.addEventListener("click", saveCurrentNotebook);
  document.getElementById("add-code-btn")?.addEventListener("click", () => addCell("code"));
  document.getElementById("add-markdown-btn")?.addEventListener("click", () => addCell("markdown"));
  document.getElementById("add-view-btn")?.addEventListener("click", () => addCell("view"));
  document.getElementById("run-all-btn")?.addEventListener("click", runAllCodeCells);
  document.getElementById("restart-kernel-btn")?.addEventListener("click", restartKernel);
  document.getElementById("sidebar-new-btn")?.addEventListener("click", createNotebook);
  document.getElementById("sidebar-refresh-btn")?.addEventListener("click", refreshNotebookList);
  document.getElementById("toggle-sidebar-btn")?.addEventListener("click", toggleSidebar);
  document.getElementById("sidebar-collapse-btn")?.addEventListener("click", toggleSidebar);
  document.getElementById("tab-files")?.addEventListener("click", () => switchSidebarTab("files"));
  document.getElementById("tab-running")?.addEventListener("click", () => switchSidebarTab("running"));
  document.getElementById("mobile-toolbar-toggle")?.addEventListener("click", () => toggleMobileToolbar());
  document.getElementById("cells-container")?.addEventListener("scroll", onCellsScroll, { passive: true });

  document.querySelectorAll(".jp-menu-item").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      event.stopPropagation();
      const menuName = btn.getAttribute("data-menu") || "菜单";
      openMenuPanel(menuName, btn);
    });
  });

  document.addEventListener("click", (event) => {
    const panel = document.getElementById("menu-panel");
    if (!panel || panel.classList.contains("hidden")) return;
    const target = event.target;
    if (!(target instanceof Element)) return;
    if (target.closest("#menu-panel") || target.closest(".jp-menu-item")) return;
    closeMenuPanel();
  });

  document.addEventListener("click", (event) => {
    const panel = document.getElementById("floating-panel");
    if (!panel || panel.classList.contains("hidden")) return;
    const target = event.target;
    if (!(target instanceof Element)) return;
    if (target.closest("#floating-panel") || target.closest(".jp-floating-dock")) return;
    closeFloatingPanel();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeMenuPanel();
      closeFloatingPanel();
      return;
    }

    if (event.key !== "Enter") {
      return;
    }

    if (!(event.shiftKey || event.ctrlKey || event.metaKey || event.altKey)) {
      return;
    }

    void handleNotebookEditorRunShortcut(event);
  });

  window.addEventListener("resize", () => {
    closeMenuPanel();
    if (window.innerWidth > 900 && state.mobileToolbarExpanded) {
      toggleMobileToolbar(false);
    }
    applyFloatingPrefs();
    requestOutputRelayout();
  });
}

async function executeMenuAction(action) {
  switch (action) {
    case "file.new":
      await createNotebook();
      break;
    case "file.save":
      await saveCurrentNotebook();
      break;
    case "edit.findReplace":
      await findAndReplaceInNotebook();
      break;
    case "edit.moveUp":
      await moveSelectedCell(-1);
      break;
    case "edit.moveDown":
      await moveSelectedCell(1);
      break;
    case "edit.mergeWithNext":
      await mergeSelectedWithNext();
      break;
    case "edit.splitAtCursor":
      await splitSelectedAtCursor();
      break;
    case "edit.deleteSelected":
      await deleteSelectedCell();
      break;
    case "view.toggleSidebar":
      toggleSidebar();
      break;
    case "view.collapseAll":
      collapseAllCells();
      break;
    case "view.expandAll":
      expandAllCells();
      break;
    case "run.selected":
      await runSelectedCell();
      break;
    case "run.all":
      await runAllCodeCells();
      break;
    case "run.clearOutputs":
      await clearAllOutputs();
      break;
    case "kernel.restart":
      await restartKernel();
      break;
    case "settings.mobileToolbar":
      toggleMobileToolbar();
      break;
    case "help.shortcuts":
      showToast("快捷提示: 在代码编辑区按 Shift+Enter 运行并跳到下一格，Ctrl(⌘)+Enter 运行并停留，Alt+Enter 运行并在下方新建", "info");
      break;
    default:
      showToast("该菜单项暂未实现", "info");
      break;
  }
}

function toggleSidebar() {
  state.sidebarCollapsed = !state.sidebarCollapsed;
  const mainLayout = document.getElementById("main-layout");
  const sidebar = document.getElementById("sidebar");
  const toggleBtn = document.getElementById("toggle-sidebar-btn");
  const insideToggle = document.getElementById("sidebar-collapse-btn");

  if (mainLayout) {
    mainLayout.classList.toggle("sidebar-collapsed", state.sidebarCollapsed);
  }
  if (sidebar) {
    sidebar.classList.toggle("collapsed", state.sidebarCollapsed);
  }
  if (toggleBtn) {
    toggleBtn.textContent = state.sidebarCollapsed ? "展开侧边栏" : "收起侧边栏";
  }
  if (insideToggle) {
    insideToggle.textContent = state.sidebarCollapsed ? "▶" : "◀";
  }

  setTimeout(requestOutputRelayout, 30);
  setTimeout(requestOutputRelayout, 220);
}

function switchSidebarTab(tabName) {
  state.sidebarTab = tabName === "running" ? "running" : "files";
  const filesTab = document.getElementById("tab-files");
  const runningTab = document.getElementById("tab-running");
  const filesPanel = document.getElementById("panel-files");
  const runningPanel = document.getElementById("panel-running");

  filesTab?.classList.toggle("active", state.sidebarTab === "files");
  runningTab?.classList.toggle("active", state.sidebarTab === "running");
  filesPanel?.classList.toggle("active", state.sidebarTab === "files");
  runningPanel?.classList.toggle("active", state.sidebarTab === "running");
}

async function callEntry(entryId, args = {}, options = {}) {
  const eid = String(entryId || "");
  const perfTrace = options && typeof options._perf === "object" ? options._perf : null;
  const forceFastLane = Boolean(options?.forceFastLane) || eid === "execute_cell";
  const bypassWorker = Boolean(options?.disableWorker)
    || eid === "execute_cell"
    || eid === "restart_kernel"
    || eid === "shutdown_kernel";

  if (!bypassWorker) {
    try {
      return await workerCallEntry(entryId, args, options);
    } catch (_workerErr) {
      // Fallback to main-thread run queue when worker is unavailable.
    }
  }

  if (perfTrace) {
    perfTrace.dispatchStartedAt = nowPerfMs();
  }
  const createResp = await fetch(`${API_BASE}/runs`, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify({
      plugin_id: state.pluginId,
      entry_id: entryId,
      args,
    }),
  });

  if (!createResp.ok) {
    throw new Error(`创建运行任务失败: HTTP ${createResp.status}`);
  }

  const run = await createResp.json();
  if (!run.run_id) {
    throw new Error("未返回运行任务编号");
  }

  if (perfTrace) {
    perfTrace.runId = String(run.run_id || "");
    perfTrace.runCreatedAt = nowPerfMs();
  }

  if (forceFastLane || bypassWorker) {
    try {
      return await waitRunResult(run.run_id, options);
    } catch (_fastLaneErr) {
      // Baseline stability first: if direct polling fails transiently,
      // fallback to shared tracker to avoid breaking run capability.
      return trackRun(run.run_id, options);
    }
  }
  return trackRun(run.run_id, options);
}

async function waitRunResult(runId, options = {}) {
  const perfTrace = options && typeof options._perf === "object" ? options._perf : null;
  const minPoll = Boolean(options?.forceFastLane) ? 30 : 80;
  const pollIntervalMs = Math.max(minPoll, Number(options.pollIntervalMs || DEFAULT_POLL_INTERVAL_MS));
  const maxWaitMs = Math.max(pollIntervalMs, Number(options.maxWaitMs || DEFAULT_MAX_WAIT_MS));
  const maxRetries = Math.max(1, Math.ceil(maxWaitMs / pollIntervalMs));

  for (let i = 0; i < maxRetries; i += 1) {
    const statusResp = await fetch(`${API_BASE}/runs/${runId}`, { headers: authHeaders() });
    if (!statusResp.ok) {
      throw new Error(`获取运行状态失败: HTTP ${statusResp.status}`);
    }

    const statusObj = await statusResp.json();
    if (perfTrace && !Number.isFinite(Number(perfTrace.firstStatusAt))) {
      perfTrace.firstStatusAt = nowPerfMs();
    }

    if (statusObj.status === "succeeded" || statusObj.status === "failed") {
      if (perfTrace) {
        perfTrace.runTerminalAt = nowPerfMs();
      }
      return readRunExport(runId, statusObj.status, perfTrace);
    }

    await sleep(pollIntervalMs);
  }

  return { success: false, data: {}, error: "等待运行结果超时" };
}

async function readRunExport(runId, runStatus, perfTrace = null) {
  if (perfTrace && !Number.isFinite(Number(perfTrace.exportRequestedAt))) {
    perfTrace.exportRequestedAt = nowPerfMs();
  }

  try {
    const exportResp = await fetch(`${API_BASE}/runs/${runId}/export`, { headers: authHeaders() });
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
          if (perfTrace) {
            perfTrace.exportReceivedAt = nowPerfMs();
          }
          const success = pluginResp.success !== false && runStatus === "succeeded";
          const data = pluginResp.data || {};
          const error = pluginResp.error?.message || pluginResp.error || (success ? "" : "任务执行失败");
          return { success, data, error };
        }
      }
    }
  } catch (_err) {
    // 忽略导出读取异常，回落到运行状态。
  }

  if (perfTrace && !Number.isFinite(Number(perfTrace.exportReceivedAt))) {
    perfTrace.exportReceivedAt = nowPerfMs();
  }

  return {
    success: runStatus === "succeeded",
    data: {},
    error: runStatus === "failed" ? "插件运行失败" : "",
  };
}

async function loadUiInfo() {
  try {
    const resp = await callEntry("get_ui_info", {});
    if (!resp.success) {
      showToast(resp.error || "读取界面信息失败", "error");
      return;
    }

    state.uiInfo = resp.data || {};

    const kernelEl = document.getElementById("kernel-state");
    if (kernelEl) {
      kernelEl.textContent = state.uiInfo?.kernel_available ? "内核已就绪" : "内核不可用";
    }

    const runtimeEl = document.getElementById("runtime-state");
    const runtimeReady = Boolean(state.uiInfo?.runtime_env?.ready);
    if (runtimeEl) {
      runtimeEl.textContent = runtimeReady ? "运行时就绪" : "运行时异常";
      runtimeEl.classList.toggle("ok", runtimeReady);
      runtimeEl.classList.toggle("bad", !runtimeReady);
      runtimeEl.title = runtimeReady ? "" : String(state.uiInfo?.runtime_env?.error || "");
    }
  } catch (err) {
    showToast(`读取界面信息失败: ${String(err)}`, "error");
  }
}

async function refreshNotebookList() {
  try {
    const resp = await callEntry("list_notebooks", {});
    if (!resp.success) {
      showToast(resp.error || "加载笔记本列表失败", "error");
      return;
    }

    state.notebooks = Array.isArray(resp.data.notebooks) ? resp.data.notebooks : [];
    renderNotebookList();
    renderRunningList();
  } catch (err) {
    showToast(`加载列表失败: ${String(err)}`, "error");
  }
}

function renderNotebookList() {
  const listEl = document.getElementById("notebook-list");
  if (!listEl) return;

  listEl.innerHTML = "";
  if (!state.notebooks.length) {
    const empty = document.createElement("div");
    empty.className = "jp-empty";
    empty.textContent = "暂无笔记本，点击新建创建。";
    listEl.appendChild(empty);
    return;
  }

  for (const item of state.notebooks) {
    const wrap = document.createElement("div");
    wrap.className = `jp-file-item ${item.path === state.currentPath ? "active" : ""}`;

    const row = document.createElement("div");
    row.className = "jp-file-row";

    const actions = document.createElement("div");
    actions.className = "jp-file-actions";

    const main = document.createElement("div");
    main.className = "jp-file-main";
    main.textContent = String(item.path || "");

    const renameBtn = document.createElement("button");
    renameBtn.type = "button";
    renameBtn.className = "jp-file-rename-btn";
    renameBtn.title = "重命名笔记本";
    renameBtn.setAttribute("aria-label", "重命名笔记本");
    renameBtn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 20h9"></path><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z"></path></svg>';
    renameBtn.addEventListener("click", async (event) => {
      event.stopPropagation();
      if (renameBtn.disabled) return;
      renameBtn.disabled = true;
      try {
        await renameNotebookFile(item.path);
      } finally {
        renameBtn.disabled = false;
      }
    });

    const delBtn = document.createElement("button");
    delBtn.type = "button";
    delBtn.className = "jp-file-delete-btn";
    delBtn.title = "删除笔记本";
    delBtn.setAttribute("aria-label", "删除笔记本");
    delBtn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 7h16"></path><path d="M9 7V5h6v2"></path><path d="M8 7l1 12h6l1-12"></path><path d="M10 11v6"></path><path d="M14 11v6"></path></svg>';
    delBtn.addEventListener("click", async (event) => {
      event.stopPropagation();
      if (delBtn.disabled) return;
      delBtn.disabled = true;
      try {
        await deleteNotebookFile(item.path);
      } finally {
        delBtn.disabled = false;
      }
    });

    const meta = document.createElement("div");
    meta.className = "jp-file-meta";
    const sizeText = Number(item.size || 0);
    meta.textContent = `${sizeText} 字节`;

    row.appendChild(main);
    actions.appendChild(renameBtn);
    actions.appendChild(delBtn);
    row.appendChild(actions);
    wrap.appendChild(row);
    wrap.appendChild(meta);
    wrap.addEventListener("click", () => openNotebook(item.path));
    listEl.appendChild(wrap);
  }
}

function parseNotebookPath(notebookPath) {
  const value = String(notebookPath || "").replace(/\\/g, "/");
  const idx = value.lastIndexOf("/");
  if (idx < 0) {
    return { dir: "", fileName: value };
  }
  return {
    dir: value.slice(0, idx),
    fileName: value.slice(idx + 1),
  };
}

function normalizeNotebookStem(stemRaw) {
  const stem = String(stemRaw || "").trim();
  if (!stem) return "";
  return stem.replace(/\.ipynb$/i, "").trim();
}

function buildRenamedNotebookPath(oldPath, nextStem) {
  const parsed = parseNotebookPath(oldPath);
  const stem = normalizeNotebookStem(nextStem);
  if (!stem) return "";
  const fileName = `${stem}.ipynb`;
  return parsed.dir ? `${parsed.dir}/${fileName}` : fileName;
}

function isInvalidNotebookFileStem(stemRaw) {
  const stem = normalizeNotebookStem(stemRaw);
  if (!stem) return true;
  if (/[<>:"/\\|?*\x00-\x1F]/.test(stem)) return true;
  if (/\.$/.test(stem)) return true;
  return false;
}

function renderRunningList() {
  const listEl = document.getElementById("running-list");
  if (!listEl) return;

  listEl.innerHTML = "";
  const runningCells = Object.entries(state.cellStatus)
    .filter(([, status]) => statusToClass(status) === "running")
    .map(([cellId]) => cellId);

  if (!runningCells.length) {
    const empty = document.createElement("div");
    empty.className = "jp-empty";
    empty.textContent = "当前没有正在运行的单元格。";
    listEl.appendChild(empty);
    return;
  }

  for (const cellId of runningCells) {
    const box = document.createElement("div");
    box.className = "jp-running-item";

    const title = document.createElement("div");
    title.className = "jp-file-main";
    title.textContent = state.currentPath || "未命名.ipynb";

    const cellIndex = (state.notebook?.cells || []).findIndex((cell) => cell.id === cellId);
    const meta = document.createElement("div");
    meta.className = "jp-file-meta";
    meta.textContent = `单元格 ${cellIndex >= 0 ? cellIndex + 1 : "?"} 正在执行`;

    box.appendChild(title);
    box.appendChild(meta);
    listEl.appendChild(box);
  }
}

async function deleteNotebookFile(notebookPath) {
  const path = String(notebookPath || "");
  if (!path) return;

  const confirmed = window.confirm("确认要删除此笔记本文件吗？删除后无法恢复");
  if (!confirmed) return;

  try {
    setUiBusy("正在删除笔记本...");
    const resp = await callEntry(
      "delete_notebook",
      {
        notebook_path: path,
      },
      applyLargeNotebookAwareOptions({
        maxWaitMs: 120000,
        pollIntervalMs: 220,
      })
    );

    if (!resp.success) {
      showToast(resp.error || "删除失败", "error");
      return;
    }

    state.notebooks = Array.isArray(resp.data?.notebooks) ? resp.data.notebooks : [];
    const deletingCurrent = String(state.currentPath || "") === path;

    if (deletingCurrent) {
      if (state.notebooks.length > 0) {
        await openNotebook(state.notebooks[0].path);
      } else {
        state.currentPath = "";
        state.currentNotebookSizeBytes = 0;
        state.largeNotebookMode = false;
        state.largeNotebookOutlineReady = false;
        state.largeNotebookInitialBatchPending = false;
        state.notebookDetailLoadProgress = 100;
        state.notebookDetailLoadLabel = "";
        state.notebook = { cells: [] };
        state.notebookTotalCells = 0;
        state.notebookLoadPromise = null;
        state.notebookLoadToken = "";
        state.selectedCellId = "";
        state.collapsedCellIds.clear();
        state.cellStatus = {};
        updateNotebookHeaderTitle();
        renderCurrentNotebook(false);
        renderRunningList();
      }
    }

    renderNotebookList();
    renderRunningList();
    showToast("删除成功", "success");
  } catch (err) {
    showToast(`删除失败: ${String(err)}`, "error");
  } finally {
    setUiBusy("");
  }
}

async function renameNotebookFile(notebookPath) {
  const oldPath = String(notebookPath || "");
  if (!oldPath) return;

  const parsed = parseNotebookPath(oldPath);
  const oldStem = normalizeNotebookStem(parsed.fileName);
  const input = window.prompt("请输入新的笔记本名称（自动保留 .ipynb 后缀）", oldStem || "");
  if (input == null) return;

  if (isInvalidNotebookFileStem(input)) {
    showToast("文件名非法：不能为空且不能包含特殊字符", "error");
    return;
  }

  const nextPath = buildRenamedNotebookPath(oldPath, input);
  if (!nextPath) {
    showToast("文件名不能为空", "error");
    return;
  }
  if (nextPath === oldPath) {
    showToast("文件名未变化", "info");
    return;
  }

  try {
    setUiBusy("正在重命名笔记本...");
    const resp = await callEntry(
      "rename_notebook",
      {
        notebook_path: oldPath,
        new_notebook_path: nextPath,
      },
      applyLargeNotebookAwareOptions({
        maxWaitMs: 120000,
        pollIntervalMs: 220,
      })
    );

    if (!resp.success) {
      showToast(resp.error || "重命名失败", "error");
      return;
    }

    state.notebooks = Array.isArray(resp.data?.notebooks) ? resp.data.notebooks : [];
    const renamedPath = String(resp.data?.renamed_path || nextPath);
    if (String(state.currentPath || "") === oldPath) {
      state.currentPath = renamedPath;
      updateNotebookHeaderTitle();
    }

    renderNotebookList();
    showToast("重命名成功", "success");
  } catch (err) {
    showToast(`重命名失败: ${String(err)}`, "error");
  } finally {
    setUiBusy("");
  }
}

async function createNotebook() {
  const raw = window.prompt("请输入笔记本名称（可含子目录）", "未命名.ipynb");
  if (!raw) return;

  try {
    const resp = await callEntry("create_notebook", { notebook_path: raw });
    if (!resp.success) {
      showToast(resp.error || "创建失败", "error");
      return;
    }

    showToast("创建成功", "success");
    await refreshNotebookList();
    await openNotebook(resp.data.notebook_path);
  } catch (err) {
    showToast(`创建失败: ${String(err)}`, "error");
  }
}

function normalizeUiCellType(cellType) {
  const value = String(cellType || "").trim().toLowerCase();
  if (value === "code") return "code";
  if (value === "view") return "view";
  return "markdown";
}

function cellLanguageByType(cellType) {
  return normalizeUiCellType(cellType) === "code" ? "python" : "markdown";
}

function isMarkdownLikeCellType(cellType) {
  const normalized = normalizeUiCellType(cellType);
  return normalized === "markdown" || normalized === "view";
}

function normalizeCell(cell, index) {
  const meta = typeof cell.metadata === "object" && cell.metadata ? { ...cell.metadata } : {};
  const cellType = normalizeUiCellType(cell.cell_type || "markdown");
  const cellId = String(cell.id || meta.id || `cell-${index + 1}`);
  const outlineOnly = Boolean(cell?.outline_only);
  const sourceText = outlineOnly ? "" : String(cell.source || "");
  const rawOutputs = outlineOnly ? [] : (Array.isArray(cell.outputs) ? cell.outputs : []);
  const outputs = rawOutputs.map((out) => compactOutputForClient(out));
  const outputText = outlineOnly ? "" : String(cell.output_text || "");
  const sourceLineCount = Math.max(1, Number(cell?.source_line_count || (sourceText ? sourceText.split(/\r\n|\r|\n/).length : 1)));
  const hasOutput = Boolean(cell?.has_output) || outputs.length > 0 || Boolean(outputText.trim());
  const outputCount = Math.max(Number(cell?.output_count || 0), outputs.length);
  meta.id = cellId;
  meta.language = String(meta.language || cellLanguageByType(cellType));

  return {
    ...cell,
    id: cellId,
    cell_type: cellType,
    metadata: meta,
    source: sourceText,
    outputs,
    output_text: outputText,
    execution_count: Number.isInteger(cell.execution_count) ? cell.execution_count : null,
    outline_only: outlineOnly,
    details_loaded: outlineOnly ? false : (cell.details_loaded !== false),
    source_line_count: sourceLineCount,
    has_output: hasOutput,
    output_count: outputCount,
  };
}

function createLocalCell(cellType, sourceText) {
  const normalizedType = normalizeUiCellType(cellType);
  const metadata = {
    id: makeLocalCellId(),
    language: cellLanguageByType(normalizedType),
  };
  return {
    id: metadata.id,
    cell_type: normalizedType,
    metadata,
    source: String(sourceText || ""),
    outputs: [],
    output_text: "",
    execution_count: null,
  };
}

function mergeNotebookCellChunk(existingCells, incomingCells) {
  const merged = Array.isArray(existingCells) ? [...existingCells] : [];
  const seen = new Set(merged.map((cell) => String(cell?.id || "")));
  for (const cell of (Array.isArray(incomingCells) ? incomingCells : [])) {
    const normalized = normalizeCell(cell, merged.length);
    const id = String(normalized.id || "");
    if (!id || seen.has(id)) continue;
    seen.add(id);
    merged.push(normalized);
  }
  return merged;
}

function mergeNotebookDetailChunk(existingCells, incomingCells) {
  const current = Array.isArray(existingCells) ? [...existingCells] : [];
  if (!current.length) {
    return mergeNotebookCellChunk([], incomingCells);
  }

  const idToIndex = new Map();
  current.forEach((cell, idx) => {
    idToIndex.set(String(cell?.id || ""), idx);
  });

  for (const rawCell of (Array.isArray(incomingCells) ? incomingCells : [])) {
    const normalized = normalizeCell(rawCell, Number(rawCell?.index || current.length));
    const id = String(normalized.id || "");
    const byIdIndex = idToIndex.get(id);

    if (Number.isInteger(byIdIndex) && byIdIndex >= 0) {
      const prev = current[byIdIndex];
      current[byIdIndex] = {
        ...prev,
        ...normalized,
        outline_only: false,
        details_loaded: true,
      };
      continue;
    }

    const byRawIndex = Number(rawCell?.index);
    if (Number.isInteger(byRawIndex) && byRawIndex >= 0 && byRawIndex < current.length) {
      current[byRawIndex] = {
        ...current[byRawIndex],
        ...normalized,
        outline_only: false,
        details_loaded: true,
      };
      idToIndex.set(id, byRawIndex);
      continue;
    }

    idToIndex.set(id, current.length);
    current.push({
      ...normalized,
      outline_only: false,
      details_loaded: true,
    });
  }

  return current;
}

async function loadNotebookOutline(path) {
  if (state.largeNotebookMode) {
    try {
      const workerResp = await workerRunTask(
        "parseNotebookOutline",
        { notebookPath: path },
        applyLargeNotebookAwareOptions({
          maxWaitMs: 300000,
          pollIntervalMs: 220,
        }),
      );
      if (workerResp && workerResp.success) {
        return workerResp.data || {};
      }
    } catch (_err) {
      // Fall back to plugin entry path when worker outline task is unavailable.
    }
  }

  const resp = await callEntry(
    "load_notebook_outline",
    {
      notebook_path: path,
    },
    applyLargeNotebookAwareOptions({
      maxWaitMs: 240000,
      pollIntervalMs: 280,
    })
  );
  if (!resp.success) {
    throw new Error(resp.error || "加载大文件轮廓失败");
  }
  return resp.data || {};
}

async function loadNotebookChunk(path, offset, limit) {
  const resp = await callEntry(
    "load_notebook",
    {
      notebook_path: path,
      cell_offset: Math.max(0, Number(offset || 0)),
      cell_limit: Math.max(1, Number(limit || NOTEBOOK_CHUNK_SIZE)),
    },
    applyLargeNotebookAwareOptions({
      maxWaitMs: 180000,
      pollIntervalMs: 240,
    })
  );
  if (!resp.success) {
    throw new Error(resp.error || "加载分片失败");
  }
  return resp.data || {};
}

async function loadNotebookCellDetail(path, index) {
  if (state.largeNotebookMode) {
    try {
      const workerResp = await workerRunTask(
        "loadCellDetail",
        {
          notebookPath: path,
          cellIndex: Math.max(0, Number(index || 0)),
        },
        applyLargeNotebookAwareOptions({
          maxWaitMs: 240000,
          pollIntervalMs: 220,
        }),
      );
      if (workerResp && workerResp.success) {
        return workerResp.data || {};
      }
    } catch (_err) {
      // Fall back to plugin entry path when worker detail task is unavailable.
    }
  }

  const resp = await callEntry(
    "load_notebook_cell_detail",
    {
      notebook_path: path,
      cell_index: Math.max(0, Number(index || 0)),
    },
    applyLargeNotebookAwareOptions({
      maxWaitMs: 180000,
      pollIntervalMs: 240,
    })
  );
  if (!resp.success) {
    throw new Error(resp.error || "加载单元格详情失败");
  }
  return resp.data || {};
}

function updateNotebookDetailProgress(loadedCount, totalCount, loading) {
  if (!state.largeNotebookMode) {
    state.notebookDetailLoadProgress = 100;
    state.notebookDetailLoadLabel = "";
    updateNotebookHeaderTitle();
    return;
  }

  const total = Math.max(1, Number(totalCount || 1));
  const loaded = Math.max(0, Math.min(total, Number(loadedCount || 0)));
  const percent = Math.round((loaded / total) * 100);
  state.notebookDetailLoadProgress = percent;
  if (loading === false && loaded < total) {
    state.notebookDetailLoadLabel = `按需加载 ${loaded}/${total}`;
  } else if (loading === false || percent >= 100) {
    state.notebookDetailLoadLabel = "细节已就绪";
  } else {
    state.notebookDetailLoadLabel = `细节加载 ${percent}%`;
  }
  updateNotebookHeaderTitle();
}

async function hydrateCellDetailsByIndex(path, index) {
  const idx = Number(index);
  if (!Number.isInteger(idx) || idx < 0) return;
  const cells = Array.isArray(state.notebook?.cells) ? state.notebook.cells : [];
  const target = cells[idx];
  if (!target || target.details_loaded !== false) return;

  const key = String(target.id || `idx-${idx}`);
  if (state.cellDetailHydrationPromises.has(key)) {
    await state.cellDetailHydrationPromises.get(key);
    return;
  }

  const task = (async () => {
    let detailCell = null;

    try {
      const detail = await loadNotebookCellDetail(path, idx);
      const detailRaw = detail?.cell && typeof detail.cell === "object"
        ? detail.cell
        : (Array.isArray(detail?.notebook?.cells) ? detail.notebook.cells[0] : null);
      if (detailRaw && typeof detailRaw === "object") {
        detailCell = {
          ...detailRaw,
          index: idx,
          outline_only: false,
          details_loaded: true,
        };
      }
    } catch (_err) {
      // Fallback to chunk API for compatibility when detail API is unavailable.
    }

    if (!detailCell) {
      const chunk = await loadNotebookChunk(path, idx, 1);
      const chunkNotebook = chunk.notebook && typeof chunk.notebook === "object" ? chunk.notebook : { cells: [] };
      const chunkCells = Array.isArray(chunkNotebook.cells) ? chunkNotebook.cells : [];
      if (!chunkCells.length) return;

      const mergedCells = mergeNotebookDetailChunk(state.notebook?.cells || [], chunkCells);
      state.notebook = {
        ...(state.notebook || {}),
        ...(chunkNotebook || {}),
        cells: mergedCells,
      };
      updateNotebookDetailProgress(
        mergedCells.filter((cell) => cell?.details_loaded !== false).length,
        state.notebookTotalCells || mergedCells.length,
        true,
      );
      return;
    }

    const currentCells = Array.isArray(state.notebook?.cells) ? [...state.notebook.cells] : [];
    const normalized = normalizeCell(detailCell, idx);
    if (idx >= 0 && idx < currentCells.length) {
      currentCells[idx] = {
        ...(currentCells[idx] || {}),
        ...normalized,
        outline_only: false,
        details_loaded: true,
      };
    } else {
      currentCells.push({
        ...normalized,
        outline_only: false,
        details_loaded: true,
      });
    }

    state.notebook = {
      ...(state.notebook || {}),
      cells: currentCells,
    };
    updateNotebookDetailProgress(
      currentCells.filter((cell) => cell?.details_loaded !== false).length,
      state.notebookTotalCells || currentCells.length,
      true,
    );
  })();

  state.cellDetailHydrationPromises.set(key, task);
  try {
    await task;
  } finally {
    if (state.cellDetailHydrationPromises.get(key) === task) {
      state.cellDetailHydrationPromises.delete(key);
    }
  }
}

async function loadNotebookRemainingInBackground(path, initialLoaded, totalCount, token) {
  let offset = Math.max(0, Number(initialLoaded || 0));
  const total = Math.max(offset, Number(totalCount || offset));
  updateNotebookDetailProgress(offset, total, true);
  while (offset < total) {
    if (state.notebookLoadToken !== token || state.currentPath !== path) {
      return;
    }

    const chunk = await loadNotebookChunk(path, offset, NOTEBOOK_CHUNK_SIZE);
    const chunkNotebook = chunk.notebook && typeof chunk.notebook === "object" ? chunk.notebook : { cells: [] };
    const chunkCells = Array.isArray(chunkNotebook.cells) ? chunkNotebook.cells : [];
    if (!chunkCells.length) {
      break;
    }

    const currentCells = Array.isArray(state.notebook?.cells) ? state.notebook.cells : [];
    const mergedCells = state.largeNotebookOutlineReady
      ? mergeNotebookDetailChunk(currentCells, chunkCells)
      : mergeNotebookCellChunk(currentCells, chunkCells);

    if (state.largeNotebookMode) {
      for (const cell of chunkCells) {
        const key = String(cell?.id || "");
        if (key) {
          state.collapsedCellIds.add(key);
        }
      }
    }

    state.notebook = {
      ...(state.notebook || {}),
      ...(chunkNotebook || {}),
      cells: mergedCells,
    };
    state.notebookTotalCells = Math.max(Number(state.notebookTotalCells || 0), Number(chunk.total_cell_count || total), mergedCells.length);
    if (state.largeNotebookOutlineReady) {
      offset += chunkCells.length;
    } else {
      offset = mergedCells.length;
    }

    updateNotebookDetailProgress(
      state.largeNotebookOutlineReady ? Math.min(total, offset) : mergedCells.length,
      total,
      true,
    );

    renderCurrentNotebook(true);
    await sleep(0);
  }

  updateNotebookDetailProgress(total, total, false);
}

async function ensureNotebookFullyLoaded() {
  if (!state.notebookLoadPromise) return;
  try {
    await state.notebookLoadPromise;
  } catch (_err) {
    // Keep current data even if background chunk load fails.
  } finally {
    state.notebookLoadPromise = null;
  }
}

async function openNotebook(path) {
  if (!path) return;
  try {
    setUiBusy("正在打开...");
    const notebookSize = getNotebookSizeByPath(path);
    state.currentPath = String(path);
    state.currentNotebookSizeBytes = notebookSize;
    state.largeNotebookMode = isLargeNotebookBySize(notebookSize);
    state.largeNotebookOutlineReady = false;
    state.largeNotebookInitialBatchPending = state.largeNotebookMode;
    state.notebookDetailLoadProgress = state.largeNotebookMode ? 0 : 100;
    state.notebookDetailLoadLabel = state.largeNotebookMode ? "正在解析元数据" : "";
    state.notebook = { cells: [] };
    state.notebookTotalCells = 0;
    updateNotebookHeaderTitle();
    renderCurrentNotebook(false);
    state.outputExpandedCellIds.clear();
    state.cellLastActiveAt.clear();
    state.cellDetailHydrationPromises.clear();

    let initialData = null;
    if (state.largeNotebookMode) {
      const outline = await loadNotebookOutline(path);
      initialData = outline;
      state.largeNotebookOutlineReady = true;
      showToast("已启用大文件模式：先展示结构，后台继续加载细节", "info");
    } else {
      initialData = await loadNotebookChunk(path, 0, NOTEBOOK_CHUNK_SIZE);
    }

    const firstChunk = initialData || {};
    state.currentPath = String(firstChunk.notebook_path || path);
    state.notebook = firstChunk.notebook || { cells: [] };
    state.notebook.cells = (state.notebook.cells || []).map((cell, index) => normalizeCell(cell, index));
    state.notebookTotalCells = Math.max(state.notebook.cells.length, Number(firstChunk.total_cell_count || state.notebook.cells.length));
    state.virtualCellHeights.clear();

    const token = `${state.currentPath}:${Date.now()}`;
    state.notebookLoadToken = token;
    state.notebookLoadPromise = null;
    const shouldBackgroundFill = (!state.largeNotebookOutlineReady) && (state.notebook.cells.length < state.notebookTotalCells);
    if (shouldBackgroundFill) {
      state.notebookLoadPromise = loadNotebookRemainingInBackground(
        state.currentPath,
        state.largeNotebookOutlineReady ? 0 : state.notebook.cells.length,
        state.notebookTotalCells,
        token,
      );
    }

    if (state.largeNotebookMode) {
      state.collapsedCellIds = new Set((state.notebook.cells || []).map((cell) => String(cell.id || "")));
    } else {
      state.collapsedCellIds.clear();
    }
    state.selectedCellId = state.notebook.cells.length ? String(state.notebook.cells[0].id) : "";
    if (state.selectedCellId) {
      touchCellActivity(state.selectedCellId);
    }
    updateNotebookDetailProgress(
      state.largeNotebookOutlineReady ? 0 : state.notebook.cells.length,
      state.notebookTotalCells,
      Boolean(state.notebookLoadPromise),
    );

    renderNotebookList();
    renderRunningList();
    renderCurrentNotebook(false);
    scheduleNotebookListRefresh(260);
  } catch (err) {
    showToast(`打开失败: ${String(err)}`, "error");
  } finally {
    setUiBusy("");
  }
}

function setSelectedCell(cellId) {
  const value = String(cellId || "");
  if (!value || state.selectedCellId === value) return;
  const prev = String(state.selectedCellId || "");
  state.selectedCellId = value;
  touchCellActivity(value);

  const prevEl = prev ? document.querySelector(`.jp-cell[data-cell-id="${prev}"]`) : null;
  const nextEl = document.querySelector(`.jp-cell[data-cell-id="${value}"]`);
  if (prevEl instanceof HTMLElement) {
    prevEl.classList.remove("jp-cell-selected");
  }
  if (nextEl instanceof HTMLElement) {
    nextEl.classList.add("jp-cell-selected");
    return;
  }

  renderCurrentNotebook(true);
}

function getSelectedCellIndex() {
  const cells = state.notebook?.cells || [];
  return cells.findIndex((cell) => String(cell.id) === String(state.selectedCellId));
}

function getSelectedCell() {
  const idx = getSelectedCellIndex();
  if (idx < 0) return null;
  return state.notebook.cells[idx] || null;
}

function ensureSelectedCell() {
  if (!state.notebook || !Array.isArray(state.notebook.cells) || !state.notebook.cells.length) {
    return null;
  }
  const selected = getSelectedCell();
  if (selected) return selected;
  state.selectedCellId = String(state.notebook.cells[0].id);
  return state.notebook.cells[0];
}

function collapseAllCells() {
  const cells = state.notebook?.cells || [];
  state.collapsedCellIds = new Set(cells.map((cell) => String(cell.id)));
  renderCurrentNotebook(true);
}

function expandAllCells() {
  state.collapsedCellIds.clear();
  renderCurrentNotebook(true);
}

function toggleCellCollapse(cellId) {
  const key = String(cellId || "");
  if (!key) return;

  if (state.collapsedCellIds.has(key)) {
    state.collapsedCellIds.delete(key);
    touchCellActivity(key);
    const idx = Array.isArray(state.notebook?.cells)
      ? state.notebook.cells.findIndex((item) => String(item?.id || "") === key)
      : -1;
    if (idx >= 0 && state.notebook?.cells?.[idx]?.details_loaded === false) {
      hydrateCellDetailsByIndex(state.currentPath, idx)
        .then(() => patchSingleCellDom(key, { keepScroll: true }))
        .catch(() => {
          // Ignore lazy hydration failures; user can retry by re-opening the cell.
        });
    }
  } else {
    state.collapsedCellIds.add(key);
  }

  patchSingleCellDom(key, { keepScroll: true });
}

function patchSingleCellDom(cellId, options = {}) {
  const key = String(cellId || "");
  if (!key) return;

  const cells = Array.isArray(state.notebook?.cells) ? state.notebook.cells : [];
  const index = cells.findIndex((cell) => String(cell.id) === key);
  if (index < 0) return;

  if (useVirtualRender(cells)) {
    renderCurrentNotebook(options.keepScroll !== false);
    return;
  }

  const cellsEl = document.getElementById("cells-container");
  const current = document.querySelector(`.jp-cell[data-cell-id="${key}"]`);
  if (!(current instanceof HTMLElement) || !(cellsEl instanceof HTMLElement)) {
    renderCurrentNotebook(options.keepScroll !== false);
    return;
  }

  const oldScrollTop = cellsEl.scrollTop;
  const replacement = renderCell(cells[index], index);
  current.replaceWith(replacement);
  if (options.keepScroll !== false) {
    cellsEl.scrollTop = oldScrollTop;
  }
  requestOutputRelayout();
}

function patchCellStatusBadge(cellId) {
  const key = String(cellId || "");
  if (!key) return;
  const statusValue = String(state.cellStatus[key] || "idle");
  const badge = document.querySelector(`.jp-cell[data-cell-id="${key}"] .jp-cell-status`);
  if (!(badge instanceof HTMLElement)) {
    return;
  }
  badge.className = `jp-cell-status ${statusToClass(statusValue)}`;
  badge.textContent = statusToLabel(statusValue);
}

function refreshVisibleCellIndices(startIndex) {
  const start = Math.max(0, Number(startIndex || 0));
  const cells = Array.isArray(state.notebook?.cells) ? state.notebook.cells : [];
  const items = Array.from(document.querySelectorAll(".jp-cell[data-cell-index]"));
  items.forEach((node) => {
    if (!(node instanceof HTMLElement)) return;
    const oldIdx = Number(node.dataset.cellIndex || -1);
    if (!Number.isFinite(oldIdx) || oldIdx < start) return;
    const newIdx = oldIdx - 1;
    row.appendChild(main);
    row.appendChild(delBtn);
    wrap.appendChild(row);
    node.dataset.cellIndex = String(newIdx);

    const tag = node.querySelector(".jp-cell-tag");
    if (tag instanceof HTMLElement) {
      tag.textContent = `单元格 ${newIdx + 1}`;
    }

    const gutter = node.querySelector(".jp-cell-gutter");
    if (gutter instanceof HTMLElement) {
      const cell = cells[newIdx];
      if (String(cell?.cell_type || "") === "code") {
        const execCount = Number.isInteger(cell?.execution_count) ? cell.execution_count : " ";
        gutter.textContent = `输入 [${execCount}]`;
      } else {
        gutter.textContent = `标记 [${newIdx + 1}]`;
      }
    }
  });
}

function autoResizeTextarea(textarea) {
  if (!textarea) return;
  textarea.style.height = "auto";
  textarea.style.height = `${textarea.scrollHeight + 2}px`;
}

function requestOutputRelayout() {
  if (state.relayoutScheduled) return;
  state.relayoutScheduled = true;

  requestAnimationFrame(() => {
    state.relayoutScheduled = false;

    const textareas = document.querySelectorAll(".jp-cell-source");
    textareas.forEach((el) => {
      if (el instanceof HTMLTextAreaElement) {
        autoResizeTextarea(el);
      }
    });

    const iframes = document.querySelectorAll(".jp-output-frame");
    iframes.forEach((el) => {
      if (!(el instanceof HTMLIFrameElement)) return;
      try {
        const win = el.contentWindow;
        if (win) {
          win.postMessage({ __nekoOutputCommand: "resize" }, "*");
        }
      } catch (_err) {
        // Ignore cross-document or timing errors; next load tick will retry.
      }
    });
  });
}

function useVirtualRender(cells) {
  return Array.isArray(cells) && cells.length >= VIRTUAL_RENDER_THRESHOLD;
}

function estimateCellHeight(cell) {
  const cellId = String(cell?.id || "");
  const measured = state.virtualCellHeights.get(cellId);
  if (Number.isFinite(measured) && measured > 32) {
    return Number(measured);
  }

  const collapsed = state.collapsedCellIds.has(cellId);
  if (collapsed) return 70;

  if (cell?.details_loaded === false) {
    return 114;
  }

  const sourceText = String(cell?.source || "");
  const lineCount = Math.max(1, sourceText.split(/\r\n|\r|\n/).length);
  if (String(cell?.cell_type || "") === "code") {
    const hasOutput = (Array.isArray(cell?.outputs) && cell.outputs.length > 0) || Boolean(String(cell?.output_text || "").trim());
    const sourceHeight = Math.min(420, 120 + lineCount * 22);
    const outputHeight = hasOutput ? (shouldDeferCellOutput(cell) ? 120 : 260) : 100;
    return 64 + sourceHeight + outputHeight;
  }

  const markdownHeight = Math.min(460, 110 + lineCount * 20);
  return 64 + markdownHeight;
}

function buildVirtualMetrics(cells) {
  const prefix = [0];
  let total = 0;
  for (const cell of cells) {
    total += estimateCellHeight(cell);
    prefix.push(total);
  }
  return { prefix, total };
}

function findVirtualIndexByOffset(prefix, offset) {
  let left = 0;
  let right = Math.max(0, prefix.length - 1);
  let ans = 0;
  const target = Math.max(0, Number(offset || 0));
  while (left <= right) {
    const mid = (left + right) >> 1;
    if (prefix[mid] <= target) {
      ans = mid;
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return Math.max(0, Math.min(ans, Math.max(0, prefix.length - 2)));
}

function getVirtualRange(cellsEl, cells) {
  if (!useVirtualRender(cells)) {
    return { start: 0, end: cells.length, padTop: 0, padBottom: 0, windowed: false };
  }

  if (state.largeNotebookMode && state.largeNotebookInitialBatchPending) {
    const firstEnd = Math.max(1, Math.min(cells.length, NOTEBOOK_LARGE_FIRST_BATCH_COUNT));
    const metrics = buildVirtualMetrics(cells);
    const padBottom = Math.max(0, Number(metrics.total || 0) - Number(metrics.prefix[firstEnd] || 0));
    return { start: 0, end: firstEnd, padTop: 0, padBottom, windowed: true };
  }

  const scrollTop = Math.max(0, Number(cellsEl.scrollTop || 0));
  const viewport = Math.max(1, Number(cellsEl.clientHeight || 0));
  const { prefix, total } = buildVirtualMetrics(cells);

  const rawStart = findVirtualIndexByOffset(prefix, scrollTop);
  const rawEnd = findVirtualIndexByOffset(prefix, scrollTop + viewport) + 1;

  const start = Math.max(0, rawStart - VIRTUAL_OVERSCAN);
  const end = Math.min(cells.length, rawEnd + VIRTUAL_OVERSCAN);
  const padTop = prefix[start] || 0;
  const padBottom = Math.max(0, total - (prefix[end] || total));
  return { start, end, padTop, padBottom, windowed: true };
}

function measureRenderedCells(cellsEl) {
  const nodes = cellsEl.querySelectorAll(".jp-cell[data-cell-id]");
  if (!nodes.length) return;
  let changed = false;
  nodes.forEach((node) => {
    if (!(node instanceof HTMLElement)) return;
    const id = String(node.dataset.cellId || "");
    if (!id) return;
    const height = Math.round(node.getBoundingClientRect().height || 0);
    if (height <= 24) return;
    const prev = Number(state.virtualCellHeights.get(id) || 0);
    if (!prev || Math.abs(prev - height) >= 10) {
      state.virtualCellHeights.set(id, height);
      changed = true;
    }
  });

  if (changed && useVirtualRender(state.notebook?.cells || [])) {
    setTimeout(() => {
      renderCurrentNotebook(true);
    }, 0);
  }
}

function onCellsScroll() {
  if (state.largeNotebookInitialBatchPending) {
    state.largeNotebookInitialBatchPending = false;
  }
  if (state.virtualScrollScheduled) return;
  state.virtualScrollScheduled = true;
  requestAnimationFrame(() => {
    state.virtualScrollScheduled = false;
    const cells = state.notebook?.cells || [];
    if (useVirtualRender(cells)) {
      renderCurrentNotebook(true);
    }
  });
}

function renderCurrentNotebook(keepScroll = true) {
  const nameEl = document.getElementById("notebook-display-name");
  const cellsEl = document.getElementById("cells-container");
  if (!cellsEl) return;

  const oldScrollTop = cellsEl.scrollTop;
  if (nameEl) {
    updateNotebookHeaderTitle();
  }

  cellsEl.innerHTML = "";
  const cells = Array.isArray(state.notebook?.cells) ? state.notebook.cells : [];
  cellsEl.classList.remove("jp-cells-has-maximized");

  if (!cells.length) {
    const empty = document.createElement("div");
    empty.className = "jp-empty";
    if (state.largeNotebookMode && String(state.notebookDetailLoadLabel || "")) {
      empty.textContent = "正在解析大文件结构，请稍候...";
    } else {
      empty.textContent = "当前笔记本没有单元格，可点击工具栏新增。";
    }
    cellsEl.appendChild(empty);
    return;
  }

  const range = getVirtualRange(cellsEl, cells);
  const fragment = document.createDocumentFragment();

  if (range.windowed && range.padTop > 0) {
    const topSpacer = document.createElement("div");
    topSpacer.className = "jp-virtual-spacer";
    topSpacer.style.height = `${Math.round(range.padTop)}px`;
    fragment.appendChild(topSpacer);
  }

  for (let index = range.start; index < range.end; index += 1) {
    fragment.appendChild(renderCell(cells[index], index));
  }

  if (range.windowed && range.padBottom > 0) {
    const bottomSpacer = document.createElement("div");
    bottomSpacer.className = "jp-virtual-spacer";
    bottomSpacer.style.height = `${Math.round(range.padBottom)}px`;
    fragment.appendChild(bottomSpacer);
  }

  cellsEl.appendChild(fragment);

  if (keepScroll) {
    cellsEl.scrollTop = oldScrollTop;
  }

  requestOutputRelayout();
  setTimeout(requestOutputRelayout, 120);
  requestAnimationFrame(() => measureRenderedCells(cellsEl));
  setTimeout(collectClientGarbage, 0);
}

function statusToClass(status) {
  const value = String(status || "idle").toLowerCase();
  if (value.includes("running") || value.includes("queue")) return "running";
  if (value.includes("success") || value === "ok" || value.includes("succeeded")) return "success";
  if (value.includes("fail") || value.includes("error") || value.includes("timeout")) return "failed";
  return "";
}

function statusToLabel(status) {
  const cls = statusToClass(status);
  if (cls === "running") return "执行中";
  if (cls === "success") return "成功";
  if (cls === "failed") return "失败";
  return "空闲";
}

function lineNumberText(text) {
  const total = Math.max(1, String(text || "").split(/\r\n|\r|\n/).length);
  return Array.from({ length: total }, (_v, idx) => String(idx + 1)).join("\n");
}

function readMimeAsText(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item)).join("");
  }
  if (value == null) {
    return "";
  }
  if (typeof value === "object") {
    try {
      return JSON.stringify(value, null, 2);
    } catch (_err) {
      return String(value);
    }
  }
  return String(value);
}

function createOutputIframe(srcdoc, className = "jp-output-frame", options = {}) {
  const minHeight = Number.isFinite(Number(options.minHeight)) ? Number(options.minHeight) : 300;
  const maxHeight = Number.isFinite(Number(options.maxHeight)) ? Number(options.maxHeight) : 760;

  const clampHeight = (value) => {
    const v = Math.round(Number(value) || minHeight);
    return Math.max(minHeight, Math.min(maxHeight, v));
  };

  const measureDocContentHeight = (doc) => {
    if (!doc || !doc.body) return minHeight;
    const body = doc.body;
    const bodyRect = body.getBoundingClientRect();
    const children = Array.from(body.children || []);

    if (!children.length) {
      const base = Math.ceil(bodyRect.height || 0);
      return clampHeight(base || minHeight);
    }

    let maxBottom = 0;
    for (const child of children) {
      const rect = child.getBoundingClientRect();
      maxBottom = Math.max(maxBottom, rect.bottom - bodyRect.top);
    }
    return clampHeight(Math.ceil(maxBottom || minHeight));
  };

  const iframe = document.createElement("iframe");
  iframe.className = className;
  iframe.setAttribute("sandbox", "allow-scripts allow-same-origin allow-downloads");
  iframe.setAttribute("loading", "lazy");
  iframe.srcdoc = srcdoc;
  iframe.style.height = `${minHeight}px`;
  iframe.style.maxHeight = `${maxHeight}px`;

  const fit = () => {
    try {
      const doc = iframe.contentDocument;
      if (!doc) return;
      const nextHeight = clampHeight(measureDocContentHeight(doc) + 8);
      iframe.style.height = `${nextHeight}px`;
    } catch (_err) {
      iframe.style.height = `${clampHeight(480)}px`;
    }
  };

  const onMessage = (event) => {
    if (!iframe.isConnected) {
      window.removeEventListener("message", onMessage);
      return;
    }
    if (event.source !== iframe.contentWindow) return;
    const payload = event.data;
    if (!payload || typeof payload !== "object") return;
    const reported = Number(payload.__nekoOutputHeight);
    if (!Number.isFinite(reported)) return;
    const next = clampHeight(reported + 8);
    iframe.style.height = `${next}px`;
  };
  window.addEventListener("message", onMessage);

  const cleanup = () => {
    window.removeEventListener("message", onMessage);
  };

  iframe.addEventListener("load", () => {
    fit();
    setTimeout(fit, 180);
    setTimeout(fit, 800);
    try {
      iframe.contentWindow?.postMessage({ __nekoOutputCommand: "resize" }, "*");
    } catch (_err) {
      // Ignore when frame is not ready.
    }
  });

  iframe.addEventListener("error", cleanup);
  iframe.addEventListener("load", () => {
    setTimeout(() => {
      if (!iframe.isConnected) cleanup();
    }, 0);
  });

  return iframe;
}

function outputBridgeScript({ includePlotlyResize = false, includePlotlyZoomBoost = false, minHeight = 300, maxHeight = 760 } = {}) {
  const plotlyResizeCode = includePlotlyResize
    ? "if (window.Plotly) { var plotEl = document.getElementById('plot'); if (plotEl) { try { window.Plotly.Plots.resize(plotEl); } catch (_err) {} } }"
    : "";

  const plotlyWheelCode = includePlotlyZoomBoost
    ? `
      function bindPlotlyZoomBoost() {
        if (!window.Plotly) return;
        var plotEl = document.getElementById("plot");
        if (!plotEl || plotEl.__nekoZoomBoostBound) return;
        plotEl.__nekoZoomBoostBound = true;

        function accelZoom(deltaY) {
          var layout = plotEl._fullLayout;
          var scene = layout && layout.scene;
          var camera = scene && scene.camera;
          var eye = camera && camera.eye;
          if (!eye) return;

          var x = Number(eye.x || 0);
          var y = Number(eye.y || 0);
          var z = Number(eye.z || 0);
          var norm = Math.sqrt(x * x + y * y + z * z);
          if (!isFinite(norm) || norm <= 0) return;

          var zoomStep = 0.26;
          var targetNorm = deltaY < 0 ? norm * (1 - zoomStep) : norm * (1 + zoomStep);
          var clampedNorm = Math.max(0.35, Math.min(9.5, targetNorm));
          var scale = clampedNorm / norm;
          var nextEye = { x: x * scale, y: y * scale, z: z * scale };
          window.Plotly.relayout(plotEl, { "scene.camera.eye": nextEye });
        }

        plotEl.addEventListener("wheel", function (evt) {
          if (!evt) return;
          evt.preventDefault();
          accelZoom(Number(evt.deltaY || 0));
          if (window.__nekoReportHeight) {
            window.__nekoReportHeight();
          }
        }, { passive: false });
      }
    `
    : "";

  return `<script>
    (function () {
      var MIN_H = ${Math.max(200, Number(minHeight) || 300)};
      var MAX_H = ${Math.max(320, Number(maxHeight) || 760)};

      function clampHeight(value) {
        var n = Math.round(Number(value) || MIN_H);
        if (n < MIN_H) n = MIN_H;
        if (n > MAX_H) n = MAX_H;
        return n;
      }

      function measureIntrinsicHeight() {
        var body = document.body;
        if (!body) return MIN_H;
        var children = body.children ? Array.prototype.slice.call(body.children) : [];
        var bodyRect = body.getBoundingClientRect();
        if (!children.length) {
          return clampHeight(Math.ceil(bodyRect.height || MIN_H));
        }

        var maxBottom = 0;
        for (var i = 0; i < children.length; i += 1) {
          var rect = children[i].getBoundingClientRect();
          maxBottom = Math.max(maxBottom, rect.bottom - bodyRect.top);
        }
        return clampHeight(Math.ceil(maxBottom || MIN_H));
      }

      function reportHeight() {
        var h = measureIntrinsicHeight();
        try {
          parent.postMessage({ __nekoOutputHeight: h }, "*");
        } catch (_err) {
          // Ignore postMessage failures in sandbox edge cases.
        }
      }
      window.__nekoReportHeight = reportHeight;

      function maybeResizePlotly() {
        ${plotlyResizeCode}
      }

      ${plotlyWheelCode}

      var reportScheduled = false;
      function scheduleReport() {
        if (reportScheduled) return;
        reportScheduled = true;
        requestAnimationFrame(function () {
          reportScheduled = false;
          reportHeight();
        });
      }

      window.addEventListener("load", function () {
        maybeResizePlotly();
        if (typeof bindPlotlyZoomBoost === "function") {
          bindPlotlyZoomBoost();
        }
        reportHeight();
        setTimeout(scheduleReport, 120);
        setTimeout(scheduleReport, 420);
        setTimeout(scheduleReport, 1000);
      });

      window.addEventListener("resize", function () {
        maybeResizePlotly();
        scheduleReport();
      });

      window.addEventListener("message", function (event) {
        var data = event && event.data;
        if (!data || data.__nekoOutputCommand !== "resize") return;
        maybeResizePlotly();
        scheduleReport();
      });

      var observer = new MutationObserver(function () {
        scheduleReport();
      });
      if (document.body) {
        observer.observe(document.body, {
          childList: true,
          subtree: true,
        });
      }

      if (window.ResizeObserver && document.body) {
        var ro = new ResizeObserver(function () {
          scheduleReport();
        });
        ro.observe(document.body);
      }
    })();
  </script>`;
}

function createHtmlOutputIframe(rawHtml) {
  const html = readMimeAsText(rawHtml);
  const srcdoc = `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    html, body {
      margin: 0;
      padding: 0;
      background: #ffffff;
      color: #1e467f;
      font-family: "Microsoft YaHei", "PingFang SC", "Noto Sans CJK SC", "Arial Unicode MS", sans-serif;
      overflow: hidden;
      max-width: 100%;
    }
    * {
      box-sizing: border-box;
      max-width: 100%;
    }
  </style>
</head>
<body>
${html}
${outputBridgeScript({ minHeight: 280, maxHeight: 760 })}
</body>
</html>`;
  return createOutputIframe(srcdoc, "jp-output-frame jp-output-frame-html", { minHeight: 280, maxHeight: 760 });
}

function createPlotlyOutputIframe(specObj) {
  const safeSpec = JSON.stringify(specObj || {}).replace(/</g, "\\u003c");
  const srcdoc = `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    html, body {
      margin: 0;
      padding: 0;
      background: #ffffff;
      overflow: hidden;
      width: 100%;
      height: 100%;
      min-height: 620px;
    }
    #plot {
      width: 100%;
      min-height: 620px;
      height: 100%;
      max-height: 1260px;
    }
  </style>
</head>
<body>
  <div id="plot"></div>
  <script>
    (function () {
      const plotEl = document.getElementById("plot");
      const spec = ${safeSpec};
      const data = Array.isArray(spec.data) ? spec.data : (Array.isArray(spec) ? spec : []);
      const layout = (spec && typeof spec === "object" && spec.layout && typeof spec.layout === "object") ? spec.layout : {};

      function calcPlotHeight() {
        var parentH = 0;
        try {
          parentH = Number(window.parent && window.parent.innerHeight) || 0;
        } catch (_err) {
          parentH = 0;
        }
        var base = parentH > 0 ? Math.round(parentH * 0.8) : Math.round(window.innerHeight * 0.9);
        return Math.max(620, Math.min(1260, base || 620));
      }

      function applyPlotHeight() {
        plotEl.style.height = String(calcPlotHeight()) + "px";
      }

      try {
        delete layout.width;
        delete layout.height;
        layout.autosize = true;
        if (!layout.font || typeof layout.font !== "object") {
          layout.font = {};
        }
        if (!layout.font.family) {
          layout.font.family = "Microsoft YaHei, SimHei, Noto Sans CJK SC, PingFang SC, Arial Unicode MS, sans-serif";
        }
        if (!layout.font.size) {
          layout.font.size = 13;
        }
        layout.margin = Object.assign({ l: 2, r: 2, t: 42, b: 2, pad: 0 }, (layout && typeof layout.margin === "object") ? layout.margin : {});

        var scene = (layout && typeof layout.scene === "object") ? layout.scene : {};
        if (!scene.aspectmode) {
          scene.aspectmode = "auto";
        }
        if (!scene.camera || typeof scene.camera !== "object") {
          scene.camera = { eye: { x: 1.55, y: 1.45, z: 0.92 } };
        } else if (!scene.camera.eye || typeof scene.camera.eye !== "object") {
          scene.camera.eye = { x: 1.55, y: 1.45, z: 0.92 };
        }
        layout.scene = scene;

        for (var i = 0; i < data.length; i += 1) {
          var trace = data[i];
          if (!trace || typeof trace !== "object") continue;
          var t = String(trace.type || "").toLowerCase();
          if (t !== "surface" && t !== "mesh3d" && t !== "scatter3d") continue;
          if (trace.showscale === false) continue;
          if (!trace.colorbar || typeof trace.colorbar !== "object") {
            trace.colorbar = {};
          }
          if (trace.colorbar.len == null) trace.colorbar.len = 0.96;
          if (trace.colorbar.thickness == null) trace.colorbar.thickness = 15;
          if (trace.colorbar.x == null) trace.colorbar.x = 0.99;
        }
      } catch (_err) {
        // Keep original layout if sanitization fails.
      }
      applyPlotHeight();
      const config = Object.assign({ responsive: true, displaylogo: false, scrollZoom: true }, (spec && typeof spec === "object" && spec.config && typeof spec.config === "object") ? spec.config : {});
      Plotly.newPlot(plotEl, data, layout, config).then(function () {
        window.dispatchEvent(new Event("resize"));
      });
      window.addEventListener("resize", function () {
        applyPlotHeight();
        Plotly.Plots.resize(plotEl);
      });
    })();
  </script>
  ${outputBridgeScript({ includePlotlyResize: true, includePlotlyZoomBoost: true, minHeight: 620, maxHeight: 1260 })}
</body>
</html>`;
  return createOutputIframe(srcdoc, "jp-output-frame jp-output-frame-plotly", { minHeight: 620, maxHeight: 1260 });
}

function createOutputImageElement(src) {
  const image = document.createElement("img");
  image.className = "jp-output-image";
  image.alt = "";
  image.decoding = "async";
  image.loading = "lazy";
  image.addEventListener("error", () => {
    image.style.display = "none";
  }, { once: true });
  image.src = String(src || "");
  return image;
}

function renderOutputItem(outputItem) {
  const wrapper = document.createElement("div");
  wrapper.className = "jp-output-item";

  if (!outputItem || typeof outputItem !== "object") {
    const text = document.createElement("pre");
    text.className = "jp-output-item-text";
    text.textContent = String(outputItem || "");
    wrapper.appendChild(text);
    return wrapper;
  }

  const outputType = String(outputItem.output_type || "");

  if (outputType === "stream") {
    const text = document.createElement("pre");
    text.className = "jp-output-item-text";
    text.textContent = readMimeAsText(outputItem.text);
    wrapper.appendChild(text);
    return wrapper;
  }

  if (outputType === "error") {
    const text = document.createElement("pre");
    text.className = "jp-output-item-text jp-output-item-error";
    const traceback = Array.isArray(outputItem.traceback) ? outputItem.traceback.join("\n") : "";
    const fallback = `${String(outputItem.ename || "Error")}: ${String(outputItem.evalue || "")}`;
    text.textContent = traceback || fallback;
    wrapper.appendChild(text);
    return wrapper;
  }

  const data = outputItem.data && typeof outputItem.data === "object" ? outputItem.data : {};
  const meta = outputItem.metadata && typeof outputItem.metadata === "object" ? outputItem.metadata : {};
  const assetRelPath = String(meta.neko_media_asset_path || "").trim();
  const assetKind = String(meta.neko_media_kind || "").toLowerCase();
  const assetMime = String(meta.neko_media_mime || "").toLowerCase();

  if (assetRelPath && (assetKind === "video" || assetMime.startsWith("video/"))) {
    const loading = document.createElement("div");
    loading.className = "jp-output-empty";
    loading.textContent = "视频加载中...";
    wrapper.appendChild(loading);

    (async () => {
      try {
        const base64 = await resolveMediaBase64FromOutput(outputItem, assetMime || "video/mp4");
        if (base64) {
          const blob = await decodeBase64ToBlobChunked(base64, assetMime || "video/mp4", { chunkChars: 192 * 1024 });
          const url = registerMediaObjectUrl(URL.createObjectURL(blob));

          const video = document.createElement("video");
          video.className = "jp-output-video";
          video.controls = true;
          video.playsInline = true;
          video.preload = "metadata";
          video.src = url;

          wrapper.innerHTML = "";
          wrapper.appendChild(video);
          return;
        }

        const fallbackSrc = String(meta.neko_media_asset_src || "").trim();
        if (fallbackSrc) {
          const video = document.createElement("video");
          video.className = "jp-output-video";
          video.controls = true;
          video.playsInline = true;
          video.preload = "metadata";
          video.src = fallbackSrc;
          wrapper.innerHTML = "";
          wrapper.appendChild(video);
          return;
        }

        const htmlValue = data["text/html"];
        if (htmlValue) {
          wrapper.innerHTML = "";
          wrapper.appendChild(createHtmlOutputIframe(htmlValue));
          return;
        }

        throw new Error("未找到视频数据");
      } catch (err) {
        loading.className = "jp-output-item-text jp-output-item-error";
        loading.textContent = `视频加载失败: ${String(err)}`;
      }
    })();
    return wrapper;
  }

  if (assetRelPath && (assetKind === "image" || assetMime.startsWith("image/"))) {
    const loading = document.createElement("div");
    loading.className = "jp-output-empty";
    loading.textContent = "图片加载中...";
    wrapper.appendChild(loading);

    (async () => {
      try {
        const base64 = await resolveMediaBase64FromOutput(outputItem, assetMime || "image/png");
        if (base64) {
          const blob = await decodeBase64ToBlobChunked(base64, assetMime || "image/png");
          const url = registerMediaObjectUrl(URL.createObjectURL(blob));
          const image = createOutputImageElement(url);
          wrapper.innerHTML = "";
          wrapper.appendChild(image);
          return;
        }

        const fallbackSrc = String(meta.neko_media_asset_src || "").trim();
        if (fallbackSrc) {
          const image = createOutputImageElement(fallbackSrc);
          wrapper.innerHTML = "";
          wrapper.appendChild(image);
          return;
        }

        const htmlValue = data["text/html"];
        if (htmlValue) {
          wrapper.innerHTML = "";
          wrapper.appendChild(createHtmlOutputIframe(htmlValue));
          return;
        }

        throw new Error("未找到图片数据");
      } catch (err) {
        loading.className = "jp-output-item-text jp-output-item-error";
        loading.textContent = `图片加载失败: ${String(err)}`;
      }
    })();
    return wrapper;
  }

  let videoMime = Object.keys(data).find((key) => String(key || "").toLowerCase().startsWith("video/"));
  if (!videoMime) {
    const metaMime = String(meta.neko_media_mime || "").toLowerCase();
    if (metaMime.startsWith("video/")) {
      videoMime = metaMime;
    }
  }

  if (videoMime) {
    const metaSize = Number(meta.neko_media_size_bytes || 0);
    const token = String(meta.neko_media_token || "");
    let mediaBytes = metaSize;
    if (mediaBytes <= 0) {
      if (token) {
        const cached = getCachedMediaPayload(token);
        mediaBytes = Number(cached?.sizeBytes || 0);
      } else {
        const rawValue = data[videoMime];
        mediaBytes = typeof rawValue === "string" ? Math.floor((rawValue.length * 3) / 4) : estimateSerializedBytes(rawValue);
      }
    }

    const hint = isOverMediaLazyThreshold(mediaBytes)
      ? `检测到大体积视频 ${formatMediaBytes(mediaBytes)}，默认未加载。`
      : "视频默认按需加载，点击按钮后开始播放。";

    const placeholder = createLazyMediaPlaceholder({
      kind: "video",
      hint,
      buttonText: "点击播放视频",
      onLoad: async () => {
        const base64 = await resolveMediaBase64FromOutput(outputItem, videoMime);
        if (!base64) {
          throw new Error("未找到视频数据");
        }
        const blob = await decodeBase64ToBlobChunked(base64, videoMime, { chunkChars: 192 * 1024 });
        const url = registerMediaObjectUrl(URL.createObjectURL(blob));

        const video = document.createElement("video");
        video.className = "jp-output-video";
        video.controls = true;
        video.playsInline = true;
        video.preload = "metadata";
        video.src = url;
        wrapper.innerHTML = "";
        wrapper.appendChild(video);

        try {
          await video.play();
        } catch (_err) {
          // Keep controls visible for manual play when autoplay is blocked.
        }
      },
    });
    wrapper.appendChild(placeholder);
    return wrapper;
  }

  if (data["application/vnd.plotly.v1+json"]) {
    const plotlySpec = data["application/vnd.plotly.v1+json"];
    const mediaBytes = estimateSerializedBytes(plotlySpec);
    if (isOverMediaLazyThreshold(mediaBytes)) {
      wrapper.appendChild(createLazyMediaPlaceholder({
        kind: "rich",
        hint: `检测到大体积交互图表 ${formatMediaBytes(mediaBytes)}，已延迟渲染。`,
        buttonText: "点击加载媒体内容",
        onLoad: async () => {
          wrapper.innerHTML = "";
          wrapper.appendChild(createPlotlyOutputIframe(plotlySpec));
        },
      }));
      return wrapper;
    }

    wrapper.appendChild(createPlotlyOutputIframe(plotlySpec));
    return wrapper;
  }

  if (data["text/html"]) {
    const htmlValue = data["text/html"];
    const mediaBytes = estimateSerializedBytes(readMimeAsText(htmlValue));
    if (isOverMediaLazyThreshold(mediaBytes)) {
      wrapper.appendChild(createLazyMediaPlaceholder({
        kind: "rich",
        hint: `检测到大体积 HTML 输出 ${formatMediaBytes(mediaBytes)}，已延迟渲染。`,
        buttonText: "点击加载媒体内容",
        onLoad: async () => {
          wrapper.innerHTML = "";
          wrapper.appendChild(createHtmlOutputIframe(htmlValue));
        },
      }));
      return wrapper;
    }

    wrapper.appendChild(createHtmlOutputIframe(htmlValue));
    return wrapper;
  }

  if (data["image/svg+xml"]) {
    const svgRaw = readMimeAsText(data["image/svg+xml"]);
    const mediaBytes = estimateSerializedBytes(svgRaw);
    if (isOverMediaLazyThreshold(mediaBytes)) {
      wrapper.appendChild(createLazyMediaPlaceholder({
        kind: "image",
        hint: `检测到大体积 SVG ${formatMediaBytes(mediaBytes)}，已延迟渲染。`,
        buttonText: "点击加载媒体内容",
        onLoad: async () => {
          wrapper.innerHTML = "";
          const svgWrap = document.createElement("div");
          svgWrap.className = "jp-output-svg";
          svgWrap.innerHTML = svgRaw;
          wrapper.appendChild(svgWrap);
        },
      }));
      return wrapper;
    }

    const svgWrap = document.createElement("div");
    svgWrap.className = "jp-output-svg";
    svgWrap.innerHTML = svgRaw;
    wrapper.appendChild(svgWrap);
    return wrapper;
  }

  if (data["image/png"] || data["image/jpeg"]) {
    const mime = data["image/png"] ? "image/png" : "image/jpeg";
    const base64 = readMimeAsText(data[mime]);
    const metaSize = Number(meta.neko_media_size_bytes || 0);
    const mediaBytes = metaSize > 0 ? metaSize : estimateBase64Bytes(base64);

    if (isOverMediaLazyThreshold(mediaBytes)) {
      wrapper.appendChild(createLazyMediaPlaceholder({
        kind: "image",
        hint: `检测到大体积图片 ${formatMediaBytes(mediaBytes)}，已延迟渲染。`,
        buttonText: "点击加载媒体内容",
        onLoad: async () => {
          const blob = await decodeBase64ToBlobChunked(base64, mime);
          const url = registerMediaObjectUrl(URL.createObjectURL(blob));
          const image = createOutputImageElement(url);
          wrapper.innerHTML = "";
          wrapper.appendChild(image);
        },
      }));
      return wrapper;
    }

    const image = createOutputImageElement(`data:${mime};base64,${base64}`);
    wrapper.appendChild(image);
    return wrapper;
  }

  const genericImageMime = Object.keys(data).find((key) => String(key || "").toLowerCase().startsWith("image/"));
  if (genericImageMime) {
    const mime = String(genericImageMime).toLowerCase();
    const base64 = readMimeAsText(data[genericImageMime]);
    const metaSize = Number(meta.neko_media_size_bytes || 0);
    const mediaBytes = metaSize > 0 ? metaSize : estimateBase64Bytes(base64);

    if (isOverMediaLazyThreshold(mediaBytes)) {
      wrapper.appendChild(createLazyMediaPlaceholder({
        kind: "image",
        hint: `检测到大体积图片 ${formatMediaBytes(mediaBytes)}，已延迟渲染。`,
        buttonText: "点击加载媒体内容",
        onLoad: async () => {
          const blob = await decodeBase64ToBlobChunked(base64, mime);
          const url = registerMediaObjectUrl(URL.createObjectURL(blob));
          const image = createOutputImageElement(url);
          wrapper.innerHTML = "";
          wrapper.appendChild(image);
        },
      }));
      return wrapper;
    }

    const image = createOutputImageElement(`data:${mime};base64,${base64}`);
    wrapper.appendChild(image);
    return wrapper;
  }

  const text = document.createElement("pre");
  text.className = "jp-output-item-text";
  const plain = data["text/plain"];
  text.textContent = plain ? readMimeAsText(plain) : JSON.stringify(data, null, 2);
  wrapper.appendChild(text);
  return wrapper;
}

function renderCellOutputs(container, cell, options = {}) {
  container.innerHTML = "";

  if (options.deferOutput === true) {
    const placeholder = document.createElement("div");
    placeholder.className = "jp-output-lazy-placeholder";

    const hint = document.createElement("div");
    hint.className = "jp-output-lazy-hint";
    hint.textContent = "该单元格输出体积较大，已延迟渲染。";

    const loadBtn = document.createElement("button");
    loadBtn.type = "button";
    loadBtn.className = "jp-output-lazy-btn";
    loadBtn.textContent = "点击加载输出";
    loadBtn.addEventListener("click", async () => {
      if (loadBtn.disabled) return;
      loadBtn.disabled = true;
      loadBtn.textContent = "加载中...";
      try {
        if (typeof options.onLoadOutput === "function") {
          await options.onLoadOutput();
        }
      } finally {
        loadBtn.disabled = false;
      }
    });

    placeholder.appendChild(hint);
    placeholder.appendChild(loadBtn);
    container.appendChild(placeholder);
    return;
  }

  const outputs = Array.isArray(cell.outputs) ? cell.outputs : [];
  if (outputs.length > 0) {
    outputs.forEach((outputItem) => {
      container.appendChild(renderOutputItem(outputItem));
    });
    return;
  }

  const fallbackText = String(cell.output_text || "").trim();
  if (fallbackText) {
    const text = document.createElement("pre");
    text.className = "jp-output-item-text";
    text.textContent = fallbackText;
    container.appendChild(text);
    return;
  }

  const empty = document.createElement("div");
  empty.className = "jp-output-empty";
  empty.textContent = "暂无输出";
  container.appendChild(empty);
}

function extractViewMediaCandidates(sourceText) {
  const text = String(sourceText || "");
  if (!text.trim()) return [];

  const refs = [];
  const mediaExtPattern = "(?:png|jpe?g|gif|webp|bmp|svg|mp4|mov|avi|mkv|flv|wmv|webm)";
  const sepClass = "[\\s\"'<>\\(\\)\\[\\]\\{\\}，,；;]";
  const linkPattern = /!?\[[^\]]*?\]\(([^)\r\n]+)\)|<([^>\r\n]+)>/g;
  let match = null;
  while ((match = linkPattern.exec(text)) !== null) {
    const candidate = String(match[1] || match[2] || "").trim();
    if (candidate) refs.push(candidate);
  }

  const assetPathPattern = new RegExp(
    `(?:^|${sepClass})((?:\\./)?(?:assets/)?(?:image|video)/[^\\s\"'<>\\(\\)\\[\\]\\{\\}，,；;]+\\.${mediaExtPattern})(?=$|${sepClass})`,
    "gi"
  );
  while ((match = assetPathPattern.exec(text)) !== null) {
    const candidate = String(match[1] || "").trim();
    if (candidate) refs.push(candidate);
  }

  const fileNamePattern = new RegExp(
    `(?:^|${sepClass})([^/\\\\\\s\"'<>\\(\\)\\[\\]\\{\\}，,；;]+\\.${mediaExtPattern})(?=$|${sepClass})`,
    "gi"
  );
  while ((match = fileNamePattern.exec(text)) !== null) {
    const candidate = String(match[1] || "").trim();
    if (candidate) refs.push(candidate);
  }

  const stripped = text.replace(linkPattern, "\n");
  const lines = stripped.split(/\r\n|\r|\n/);
  for (const rawLine of lines) {
    let line = String(rawLine || "").trim();
    if (!line) continue;
    line = line.replace(/^[-*+]\s+/, "").replace(/^\d+\.\s+/, "").trim();
    if (!line) continue;

    const parts = line.split(",").map((part) => String(part || "").trim()).filter(Boolean);
    if (parts.length > 1) {
      refs.push(...parts);
    } else {
      refs.push(line);
    }
  }

  return refs;
}

function normalizeViewAssetReference(rawRef) {
  let value = String(rawRef || "").trim();
  if (!value) return null;

  value = value
    .replace(/^`+|`+$/g, "")
    .replace(/^["']+|["']+$/g, "")
    .replace(/^[\s<>{}\[\]()，,；;。！？!?:：]+/g, "")
    .replace(/[\s<>{}\[\]()，,；;。！？!?:：]+$/g, "")
    .trim();
  if (!value) return null;

  value = value.split("?")[0].split("#")[0].trim();
  if (!value) return null;

  value = value.replace(/\\/g, "/").replace(/^\.\/+/, "").replace(/^\/+/, "");
  if (!value) return null;

  const segments = value.split("/").map((seg) => String(seg || "").trim()).filter(Boolean);
  if (!segments.length) return null;

  const fileName = sanitizeFileNameForFs(segments[segments.length - 1]);
  if (!fileName) return null;

  const ext = fileExtLower(fileName);
  const inferredMime = inferUploadMimeFromExt(ext);
  const inferredKind = mediaKindFromMime(inferredMime);

  let kind = "";
  const firstSeg = String(segments[0] || "").toLowerCase();
  if (firstSeg === "assets" && segments.length >= 3) {
    const secondSeg = String(segments[1] || "").toLowerCase();
    if (secondSeg === "image" || secondSeg === "video") {
      kind = secondSeg;
    }
  }
  if (!kind && (firstSeg === "image" || firstSeg === "video")) {
    kind = firstSeg;
  }
  if (!kind) {
    kind = inferredKind;
  }
  if (!(kind === "image" || kind === "video")) return null;

  const mime = inferredMime || (kind === "video" ? "video/mp4" : "image/png");
  return {
    relPath: `assets/${kind}/${fileName}`,
    fileName,
    kind,
    mime,
  };
}

function buildViewCellMediaOutputs(sourceText, notebookPath) {
  const refs = extractViewMediaCandidates(sourceText);
  const outputs = [];
  const seen = new Set();

  for (const ref of refs) {
    const normalized = normalizeViewAssetReference(ref);
    if (!normalized) continue;

    const dedupeKey = String(normalized.relPath || "").toLowerCase();
    if (!dedupeKey || seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);

    outputs.push({
      output_type: "display_data",
      data: {},
      metadata: {
        neko_media_mime: normalized.mime,
        neko_media_kind: normalized.kind,
        neko_media_asset_path: normalized.relPath,
        neko_media_asset_src: `./${normalized.relPath}`,
        neko_media_original_name: normalized.fileName,
        neko_media_saved_name: normalized.fileName,
        neko_notebook_path: String(notebookPath || state.currentPath || ""),
      },
    });
  }

  return outputs;
}

function renderViewCellPreview(container, cell) {
  container.innerHTML = "";
  const outputs = buildViewCellMediaOutputs(String(cell?.source || ""), String(state.currentPath || ""));

  if (!outputs.length) {
    const empty = document.createElement("div");
    empty.className = "jp-output-empty";
    empty.textContent = "请输入媒体路径（可夹带备注），例如 标签A assets/image/demo.png 说明文字";
    container.appendChild(empty);
    return;
  }

  renderCellOutputs(container, {
    outputs,
    output_text: "",
  });
}

const CELL_TOOL_SVG = {
  upload: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 16V5"></path><path d="M8 9l4-4 4 4"></path><rect x="4" y="16" width="16" height="4" rx="1.2"></rect></svg>',
  paste: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M9 4h6"></path><path d="M9 7h6"></path><path d="M8 3h8a1 1 0 0 1 1 1v3H7V4a1 1 0 0 1 1-1z"></path><rect x="5" y="7" width="14" height="14" rx="2"></rect><path d="M12 11v6"></path><path d="M9 14h6"></path></svg>',
  up: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 18V6"></path><path d="M7 11l5-5 5 5"></path></svg>',
  down: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 6v12"></path><path d="M7 13l5 5 5-5"></path></svg>',
  addBelow: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="4" y="5" width="16" height="5" rx="1"></rect><path d="M12 13v6"></path><path d="M9 16h6"></path></svg>',
  addAbove: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 5v6"></path><path d="M9 8h6"></path><rect x="4" y="14" width="16" height="5" rx="1"></rect></svg>',
  delete: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 7h16"></path><path d="M9 7V5h6v2"></path><path d="M8 7l1 12h6l1-12"></path><path d="M10 11v6"></path><path d="M14 11v6"></path></svg>',
};

function createCellToolButton(iconKey, title, onClick, extraClass = "") {
  const btn = document.createElement("button");
  btn.className = `jp-cell-tool-btn ${extraClass}`.trim();
  btn.type = "button";
  btn.title = title;
  btn.setAttribute("aria-label", title);
  btn.innerHTML = CELL_TOOL_SVG[iconKey] || "";
  btn.addEventListener("click", async (event) => {
    event.stopPropagation();
    if (btn.disabled) return;
    btn.disabled = true;
    btn.classList.add("is-pending");
    try {
      await onClick();
    } catch (err) {
      showToast(`${title}失败: ${String(err)}`, "error");
    } finally {
      btn.classList.remove("is-pending");
      btn.disabled = false;
    }
  });
  return btn;
}

function renderCell(cell, index) {
  const article = document.createElement("article");
  const selected = String(state.selectedCellId) === String(cell.id);
  const isCollapsed = state.collapsedCellIds.has(String(cell.id));
  const detailsLoaded = cell.details_loaded !== false;
  const normalizedCellType = normalizeUiCellType(cell.cell_type || "markdown");
  article.className = `jp-cell ${isMarkdownLikeCellType(normalizedCellType) ? "jp-cell-markdown" : ""} ${selected ? "jp-cell-selected" : ""}`;
  article.dataset.cellId = String(cell.id);
  article.dataset.cellIndex = String(index);
  article.addEventListener("click", () => setSelectedCell(cell.id));

  const gutter = document.createElement("div");
  gutter.className = "jp-cell-gutter";
  const prompt = normalizedCellType === "code"
    ? `输入 [${Number.isInteger(cell.execution_count) ? cell.execution_count : " "}]`
    : (normalizedCellType === "view" ? `视图 [${index + 1}]` : `标记 [${index + 1}]`);
  gutter.textContent = prompt;

  const main = document.createElement("div");
  main.className = "jp-cell-main";

  const head = document.createElement("div");
  head.className = "jp-cell-head";

  const headLeft = document.createElement("div");
  headLeft.className = "jp-cell-head-left";

  const tag = document.createElement("span");
  tag.className = "jp-cell-tag";
  tag.textContent = `单元格 ${index + 1}`;

  const typeSelect = document.createElement("select");
  typeSelect.className = "jp-cell-type";
  typeSelect.innerHTML = "<option value=\"code\">代码</option><option value=\"markdown\">标记</option><option value=\"view\">视图</option>";
  typeSelect.value = normalizedCellType;
  typeSelect.addEventListener("click", (event) => event.stopPropagation());
  typeSelect.addEventListener("change", async () => {
    touchCellActivity(cell.id);
    cell.cell_type = normalizeUiCellType(typeSelect.value);
    cell.metadata.language = cellLanguageByType(cell.cell_type);
    if (cell.cell_type !== "code") {
      cell.outputs = [];
      cell.output_text = "";
      cell.execution_count = null;
    }
    await persistNotebookStructure(true);
    renderCurrentNotebook(true);
  });

  const status = state.cellStatus[cell.id] || "idle";
  const statusTag = document.createElement("span");
  statusTag.className = `jp-cell-status ${statusToClass(status)}`;
  statusTag.textContent = statusToLabel(status);

  const hasEmbeddedVideo = cellHasEmbeddedVideo(cell);
  const mediaTag = document.createElement("span");
  mediaTag.className = "jp-cell-media-chip";
  mediaTag.textContent = "已内嵌视频";

  const collapseBtn = document.createElement("button");
  collapseBtn.className = "jp-cell-btn collapse";
  collapseBtn.textContent = isCollapsed ? "展开" : "折叠";
  collapseBtn.addEventListener("click", async (event) => {
    event.stopPropagation();
    try {
      await flushPendingCellUpdate(cell.id);
    } catch (_err) {
      // 折叠操作不应因自动同步失败而阻塞。
    }
    toggleCellCollapse(cell.id);
  });

  headLeft.appendChild(tag);
  headLeft.appendChild(typeSelect);
  headLeft.appendChild(statusTag);
  if (isCollapsed && hasEmbeddedVideo) {
    headLeft.appendChild(mediaTag);
  }
  headLeft.appendChild(collapseBtn);
  if (normalizedCellType === "code") {
    const runBtn = document.createElement("button");
    runBtn.className = "jp-cell-btn run";
    runBtn.textContent = "运行";
    runBtn.addEventListener("click", async (event) => {
      event.stopPropagation();
      if (runBtn.disabled) return;
      runBtn.disabled = true;
      touchCellActivity(cell.id);
      setSelectedCell(cell.id);
      try {
        await runCell(cell.id, {
          clickAt: nowPerfMs(),
          trigger: "run_button",
        });
      } finally {
        runBtn.disabled = false;
      }
    });
    headLeft.appendChild(runBtn);
  }

  const headRight = document.createElement("div");
  headRight.className = "jp-cell-head-right";

  const tools = document.createElement("div");
  tools.className = "jp-cell-tools";
  if (normalizedCellType === "code") {
    tools.appendChild(createCellToolButton("upload", "上传文件到输出", async () => {
      setSelectedCell(cell.id);
      await uploadFilesToCellOutput(cell.id);
    }));
    tools.appendChild(createCellToolButton("paste", "粘贴图片到输出", async () => {
      setSelectedCell(cell.id);
      await pasteClipboardMediaToCellOutput(cell.id);
    }));
  } else if (normalizedCellType === "view") {
    tools.appendChild(createCellToolButton("upload", "上传媒体并回填文件名", async () => {
      setSelectedCell(cell.id);
      await uploadFilesToViewCellSource(cell.id);
    }));
  }
  tools.appendChild(createCellToolButton("up", "上移单元格", async () => {
    await moveCellById(cell.id, -1);
  }));
  tools.appendChild(createCellToolButton("down", "下移单元格", async () => {
    await moveCellById(cell.id, 1);
  }));
  tools.appendChild(createCellToolButton("addBelow", "在下方新建单元格", async () => {
    await insertCellRelative(cell.id, "below");
  }));
  tools.appendChild(createCellToolButton("addAbove", "在上方新建单元格", async () => {
    await insertCellRelative(cell.id, "above");
  }));
  tools.appendChild(createCellToolButton("delete", "删除单元格", async () => {
    await deleteCell(cell.id, { confirm: false });
  }, "delete"));
  headRight.appendChild(tools);

  head.appendChild(headLeft);
  head.appendChild(headRight);
  main.appendChild(head);

  if (!isCollapsed) {
    if (normalizedCellType === "code") {
      if (!detailsLoaded) {
        const pending = document.createElement("div");
        pending.className = "jp-cell-pending-details";
        pending.textContent = "该单元格内容按需加载，展开或运行时会自动补全。";
        main.appendChild(pending);
      } else {
        const sourceWrap = document.createElement("div");
        sourceWrap.className = `jp-source-wrap ${statusToClass(status) === "running" ? "is-running" : ""}`;

        const lineNumbers = document.createElement("pre");
        lineNumbers.className = "jp-line-numbers";
        lineNumbers.textContent = lineNumberText(cell.source || "");

        const source = document.createElement("textarea");
        source.className = "jp-cell-source";
        source.dataset.cellId = String(cell.id);
        source.value = cell.source || "";
        source.placeholder = "输入代码";
        source.addEventListener("click", (event) => event.stopPropagation());
        source.addEventListener("focus", () => setSelectedCell(cell.id));
        source.addEventListener("scroll", () => {
          lineNumbers.scrollTop = source.scrollTop;
        });
        source.addEventListener("input", () => {
          touchCellActivity(cell.id);
          cell.source = source.value;
          cell.details_loaded = true;
          cell.outline_only = false;
          cell.source_line_count = Math.max(1, source.value.split(/\r\n|\r|\n/).length);
          lineNumbers.textContent = lineNumberText(source.value);
          autoResizeTextarea(source);
          scheduleCellUpdate(cell.id);
        });

        source.addEventListener("click", () => touchCellActivity(cell.id));

        sourceWrap.appendChild(lineNumbers);
        sourceWrap.appendChild(source);
        main.appendChild(sourceWrap);
        autoResizeTextarea(source);
        requestAnimationFrame(() => autoResizeTextarea(source));
        setTimeout(() => autoResizeTextarea(source), 90);

        const outputWrap = document.createElement("div");
        outputWrap.className = `jp-output-block ${statusToClass(status) === "running" ? "is-running" : ""}`;

        const outputTitle = document.createElement("div");
        outputTitle.className = "jp-output-title";
        outputTitle.textContent = `输出（执行序号: ${Number.isInteger(cell.execution_count) ? cell.execution_count : "-"}）`;
        if (statusToClass(status) === "running") {
          const running = document.createElement("span");
          running.className = "jp-inline-running";
          running.textContent = "正在运行";
          outputTitle.appendChild(running);
        }

        const output = document.createElement("div");
        output.className = "jp-cell-output";
        renderCellOutputs(output, cell, {
          deferOutput: shouldDeferCellOutput(cell),
          onLoadOutput: async () => {
            touchCellActivity(cell.id);
            state.outputExpandedCellIds.add(String(cell.id));
            patchSingleCellDom(cell.id, { keepScroll: true });
          },
        });

        outputWrap.appendChild(outputTitle);
        outputWrap.appendChild(output);
        main.appendChild(outputWrap);
      }
    } else if (normalizedCellType === "markdown") {
      if (!detailsLoaded) {
        const pending = document.createElement("div");
        pending.className = "jp-cell-pending-details";
        pending.textContent = "该标记单元格内容按需加载，展开后会自动补全。";
        main.appendChild(pending);
      } else {
        const source = document.createElement("textarea");
        source.className = "jp-cell-source jp-cell-source-markdown";
        source.dataset.cellId = String(cell.id);
        source.value = cell.source || "";
        source.placeholder = "输入标记文本";
        source.addEventListener("click", (event) => event.stopPropagation());
        source.addEventListener("focus", () => setSelectedCell(cell.id));
        source.addEventListener("input", () => {
          touchCellActivity(cell.id);
          cell.source = source.value;
          cell.details_loaded = true;
          cell.outline_only = false;
          cell.source_line_count = Math.max(1, source.value.split(/\r\n|\r|\n/).length);
          autoResizeTextarea(source);
          scheduleCellUpdate(cell.id);
          const preview = article.querySelector(".jp-markdown-preview");
          if (preview) {
            preview.innerHTML = markdownToHtml(cell.source || "");
          }
        });

        source.addEventListener("click", () => touchCellActivity(cell.id));
        main.appendChild(source);
        autoResizeTextarea(source);
        requestAnimationFrame(() => autoResizeTextarea(source));
        setTimeout(() => autoResizeTextarea(source), 90);

        const preview = document.createElement("div");
        preview.className = "jp-markdown-preview";
        preview.innerHTML = markdownToHtml(cell.source || "");
        main.appendChild(preview);
      }
    } else {
      if (!detailsLoaded) {
        const pending = document.createElement("div");
        pending.className = "jp-cell-pending-details";
        pending.textContent = "该视图单元格内容按需加载，展开后会自动补全。";
        main.appendChild(pending);
      } else {
        const source = document.createElement("textarea");
        source.className = "jp-cell-source jp-cell-source-markdown";
        source.dataset.cellId = String(cell.id);
        source.value = cell.source || "";
        source.placeholder = "输入媒体路径并可附带备注，例如：素材A assets/image/demo.png 今日样张";
        source.addEventListener("click", (event) => event.stopPropagation());
        source.addEventListener("focus", () => setSelectedCell(cell.id));

        const outputWrap = document.createElement("div");
        outputWrap.className = "jp-output-block";

        const outputTitle = document.createElement("div");
        outputTitle.className = "jp-output-title";
        outputTitle.textContent = "视图输出（按路径识别 assets/image|video，支持备注文本）";

        const preview = document.createElement("div");
        preview.className = "jp-cell-output";

        source.addEventListener("input", () => {
          touchCellActivity(cell.id);
          cell.source = source.value;
          cell.details_loaded = true;
          cell.outline_only = false;
          cell.source_line_count = Math.max(1, source.value.split(/\r\n|\r|\n/).length);
          autoResizeTextarea(source);
          scheduleCellUpdate(cell.id);
          renderViewCellPreview(preview, cell);
        });

        source.addEventListener("click", () => touchCellActivity(cell.id));
        main.appendChild(source);
        autoResizeTextarea(source);
        requestAnimationFrame(() => autoResizeTextarea(source));
        setTimeout(() => autoResizeTextarea(source), 90);

        renderViewCellPreview(preview, cell);
        outputWrap.appendChild(outputTitle);
        outputWrap.appendChild(preview);
        main.appendChild(outputWrap);
      }
    }
  }

  article.appendChild(gutter);
  article.appendChild(main);
  return article;
}

function scheduleCellUpdate(cellId) {
  if (updateTimers.has(cellId)) {
    clearTimeout(updateTimers.get(cellId));
  }

  const timer = setTimeout(() => {
    persistCellUpdate(cellId, true).catch((err) => {
      showToast(`自动同步失败: ${String(err)}`, "error");
    });
  }, 700);
  updateTimers.set(cellId, timer);
}

async function flushPendingCellUpdate(cellId) {
  if (!state.currentPath || !state.notebook) return;
  const key = String(cellId || "");
  if (!key) return;

  if (updateTimers.has(key)) {
    clearTimeout(updateTimers.get(key));
    updateTimers.delete(key);
  }

  const cell = (state.notebook.cells || []).find((item) => String(item.id) === key);
  if (!cell) return;

  await persistCellUpdate(key, false);
}

async function persistCellUpdate(cellId, emitEvent = true) {
  if (!state.currentPath || !state.notebook) return;
  const syncKey = String(cellId || "");
  if (!syncKey) return;

  if (state.cellSyncPromises.has(syncKey)) {
    await state.cellSyncPromises.get(syncKey);
    return;
  }

  const task = (async () => {
    const idx = (state.notebook.cells || []).findIndex((item) => String(item.id) === String(cellId));
    const cell = idx >= 0 ? state.notebook.cells[idx] : null;
    if (!cell) return;

    const resp = await callEntry(
      "update_cell",
      {
        notebook_path: state.currentPath,
        cell_id: syncKey,
        cell_index: idx,
        source: String(cell.source || ""),
        cell_type: cell.cell_type || "markdown",
        emit_event: emitEvent === true,
        include_notebook: false,
      },
      applyLargeNotebookAwareOptions({
        maxWaitMs: 45000,
        pollIntervalMs: 200,
        priority: 5,
      })
    );

    if (!resp.success) {
      throw new Error(resp.error || "更新失败");
    }

    const notebookObj = resp.data?.notebook;
    if (notebookObj && typeof notebookObj === "object") {
      state.notebook = notebookObj;
      state.notebook.cells = (state.notebook.cells || []).map((item, index) => normalizeCell(item, index));
    }
    scheduleNotebookListRefresh(1200);
  })();

  state.cellSyncPromises.set(syncKey, task);
  try {
    await task;
  } finally {
    if (state.cellSyncPromises.get(syncKey) === task) {
      state.cellSyncPromises.delete(syncKey);
    }
  }
}

async function persistNotebookStructure(silent = false, callOptions = null) {
  if (!state.currentPath || !state.notebook) {
    if (!silent) showToast("请先打开笔记本", "error");
    return false;
  }

  await ensureNotebookFullyLoaded();

  const cells = (state.notebook.cells || []).map((cell, index) => {
    const normalized = normalizeCell(cell, index);
    return {
      cell_type: normalized.cell_type,
      metadata: {
        ...normalized.metadata,
        id: normalized.id,
        language: cellLanguageByType(normalized.cell_type),
      },
      source: normalized.source,
      outputs: Array.isArray(normalized.outputs) ? normalized.outputs.map((out) => materializeOutputForPersist(out)) : [],
      execution_count: Number.isInteger(normalized.execution_count) ? normalized.execution_count : null,
    };
  });

  const resp = await callEntry(
    "save_notebook",
    {
      notebook_path: state.currentPath,
      cells,
      metadata: state.notebook.metadata || {},
    },
    (callOptions && typeof callOptions === "object") ? callOptions : {}
  );

  if (!resp.success) {
    if (!silent) showToast(resp.error || "保存失败", "error");
    return false;
  }

  state.notebook = resp.data.notebook || state.notebook;
  state.notebook.cells = (state.notebook.cells || []).map((cell, index) => normalizeCell(cell, index));
  scheduleNotebookListRefresh(260);
  if (!silent) showToast("保存成功", "success");
  return true;
}

async function saveCurrentNotebook() {
  const ok = await persistNotebookStructure(false);
  if (!ok) return;
  renderCurrentNotebook(true);
  await refreshNotebookList();
}

async function addCell(cellType, options = {}) {
  if (!state.currentPath) {
    showToast("请先打开笔记本", "error");
    return;
  }

  const index = Number.isFinite(Number(options.index)) ? Number(options.index) : -1;
  const source = typeof options.source === "string" ? options.source : "";
  const silent = Boolean(options.silent);

  try {
    const resp = await callEntry("add_cell", {
      notebook_path: state.currentPath,
      cell_type: cellType,
      source,
      index,
    });

    if (!resp.success) {
      if (!silent) showToast(resp.error || "新增失败", "error");
      return;
    }

    state.notebook = resp.data.notebook || state.notebook;
    state.notebook.cells = (state.notebook.cells || []).map((cell, index) => normalizeCell(cell, index));
    const createdId = String(resp.data?.cell_id || "");
    if (createdId) {
      state.selectedCellId = createdId;
    } else {
      const fallbackIndex = index < 0
        ? state.notebook.cells.length - 1
        : Math.max(0, Math.min(index, state.notebook.cells.length - 1));
      const next = state.notebook.cells[fallbackIndex];
      if (next) state.selectedCellId = String(next.id);
    }
    renderCurrentNotebook(index >= 0);
    scheduleNotebookListRefresh(320);
    if (!silent) showToast("新增成功", "success");
    return createdId;
  } catch (err) {
    if (!silent) showToast(`新增失败: ${String(err)}`, "error");
    return "";
  }
}

async function blobToDataUrl(blob) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(reader.error || new Error("读取剪贴板数据失败"));
    reader.readAsDataURL(blob);
  });
}

function imageOutputFromDataUrl(mime, dataUrl) {
  const marker = ",";
  const at = String(dataUrl || "").indexOf(marker);
  const base64 = at >= 0 ? String(dataUrl).slice(at + 1) : "";
  return mediaOutputFromBase64(mime, base64);
}

function mediaKindFromMime(mime) {
  const value = String(mime || "").toLowerCase();
  if (value.startsWith("video/")) return "video";
  if (value.startsWith("image/")) return "image";
  return "";
}

function defaultNameByMime(mime) {
  const value = String(mime || "").toLowerCase();
  if (value === "video/mp4") return "clipboard.mp4";
  if (value === "video/webm") return "clipboard.webm";
  if (value === "video/quicktime") return "clipboard.mov";
  if (value === "video/x-msvideo") return "clipboard.avi";
  if (value === "video/x-matroska") return "clipboard.mkv";
  if (value === "video/x-flv") return "clipboard.flv";
  if (value === "video/x-ms-wmv") return "clipboard.wmv";
  if (value === "image/png") return "clipboard.png";
  if (value === "image/jpeg") return "clipboard.jpg";
  if (value === "image/gif") return "clipboard.gif";
  if (value === "image/webp") return "clipboard.webp";
  if (value === "image/bmp") return "clipboard.bmp";
  if (value === "image/svg+xml") return "clipboard.svg";
  return "clipboard.bin";
}

function shouldPersistAsAsset(mime, sizeBytes) {
  const kind = mediaKindFromMime(mime);
  if (kind === "image") return Number(sizeBytes || 0) > INLINE_IMAGE_MAX_BYTES;
  return false;
}

function fileExtLower(fileName) {
  const name = String(fileName || "").trim().toLowerCase();
  const idx = name.lastIndexOf(".");
  return idx >= 0 ? name.slice(idx) : "";
}

function inferUploadMimeFromExt(ext) {
  if (ext === ".png") return "image/png";
  if (ext === ".jpg" || ext === ".jpeg") return "image/jpeg";
  if (ext === ".gif") return "image/gif";
  if (ext === ".webp") return "image/webp";
  if (ext === ".bmp") return "image/bmp";
  if (ext === ".svg") return "image/svg+xml";
  if (ext === ".mp4") return "video/mp4";
  if (ext === ".mov") return "video/quicktime";
  if (ext === ".avi") return "video/x-msvideo";
  if (ext === ".mkv") return "video/x-matroska";
  if (ext === ".flv") return "video/x-flv";
  if (ext === ".wmv") return "video/x-ms-wmv";
  if (ext === ".webm") return "video/webm";
  return "";
}

function normalizeUploadMime(file) {
  const mimeRaw = String(file?.type || "").trim().toLowerCase();
  if (UPLOAD_IMAGE_MIME_LIST.includes(mimeRaw) || UPLOAD_VIDEO_MIME_LIST.includes(mimeRaw)) {
    return mimeRaw;
  }
  const ext = fileExtLower(file?.name || "");
  const inferred = inferUploadMimeFromExt(ext);
  return inferred || mimeRaw;
}

function isSupportedUploadFile(file) {
  const mime = normalizeUploadMime(file);
  if (UPLOAD_IMAGE_MIME_LIST.includes(mime) || UPLOAD_VIDEO_MIME_LIST.includes(mime)) {
    return true;
  }
  const ext = fileExtLower(file?.name || "");
  return UPLOAD_IMAGE_EXT_LIST.includes(ext) || UPLOAD_VIDEO_EXT_LIST.includes(ext);
}

function pickUploadFiles() {
  return new Promise((resolve) => {
    const input = document.createElement("input");
    input.type = "file";
    input.accept = [...UPLOAD_IMAGE_EXT_LIST, ...UPLOAD_VIDEO_EXT_LIST].join(",");
    input.multiple = true;

    input.addEventListener("change", () => {
      const files = Array.from(input.files || []);
      resolve(files);
    }, { once: true });

    input.click();
  });
}

function fileToBase64WithProgress(file, onProgress) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onprogress = (event) => {
      if (!event.lengthComputable) return;
      const pct = Math.max(0, Math.min(100, Math.round((event.loaded / Math.max(1, event.total)) * 100)));
      if (typeof onProgress === "function") {
        onProgress(pct);
      }
    };
    reader.onload = () => {
      const dataUrl = String(reader.result || "");
      const at = dataUrl.indexOf(",");
      const base64 = at >= 0 ? dataUrl.slice(at + 1) : "";
      resolve(normalizeBase64Text(base64));
    };
    reader.onerror = () => reject(reader.error || new Error("读取上传文件失败"));
    reader.readAsDataURL(file);
  });
}

function supportsFsAccessApi() {
  return typeof window !== "undefined" && typeof window.showDirectoryPicker === "function";
}

function isFsAccessDeniedError(err) {
  const name = String(err?.name || "");
  const message = String(err?.message || err || "").toLowerCase();
  return name === "NotAllowedError"
    || name === "AbortError"
    || message.includes("permission")
    || message.includes("denied")
    || message.includes("notallowed");
}

function pad2(value) {
  return String(Math.max(0, Number(value || 0))).padStart(2, "0");
}

function nowTimestampText() {
  const d = new Date();
  return `${d.getFullYear()}${pad2(d.getMonth() + 1)}${pad2(d.getDate())}${pad2(d.getHours())}${pad2(d.getMinutes())}${pad2(d.getSeconds())}`;
}

function sanitizeFileNameForFs(name) {
  const raw = String(name || "").trim();
  const normalized = raw.replace(/[\\/]/g, "_").replace(/[\x00-\x1F<>:\"/\\|?*]/g, "_").replace(/\s+/g, " ").trim();
  return normalized || "asset";
}

function splitFileName(name, defaultExt = "") {
  const safe = sanitizeFileNameForFs(name);
  const idx = safe.lastIndexOf(".");
  if (idx > 0 && idx < safe.length - 1) {
    return {
      stem: safe.slice(0, idx),
      ext: safe.slice(idx),
    };
  }
  return {
    stem: safe,
    ext: String(defaultExt || ""),
  };
}

function appendSuffixToName(name, suffix) {
  const safe = sanitizeFileNameForFs(name);
  const idx = safe.lastIndexOf(".");
  if (idx > 0 && idx < safe.length - 1) {
    return `${safe.slice(0, idx)}${suffix}${safe.slice(idx)}`;
  }
  return `${safe}${suffix}`;
}

function buildTimestampedFileName(originalName, defaultExt = "") {
  const parts = splitFileName(originalName, defaultExt);
  const safeStem = String(parts.stem || "asset").trim() || "asset";
  return `${safeStem}_${nowTimestampText()}${String(parts.ext || "")}`;
}

async function ensureAuthorizedMediaRootHandle() {
  if (!supportsFsAccessApi()) {
    throw new Error("File System Access API unavailable");
  }

  const existing = state.mediaFsDirHandle;
  if (existing) {
    try {
      const current = await existing.queryPermission({ mode: "readwrite" });
      if (current === "granted") {
        return existing;
      }
      const requested = await existing.requestPermission({ mode: "readwrite" });
      if (requested === "granted") {
        return existing;
      }
    } catch (_err) {
      // Re-select directory when stale handle is invalid.
    }
    state.mediaFsDirHandle = null;
  }

  const picked = await window.showDirectoryPicker({ mode: "readwrite" });
  const requested = await picked.requestPermission({ mode: "readwrite" });
  if (requested !== "granted") {
    throw new Error("Directory permission denied");
  }

  state.mediaFsDirHandle = picked;
  return picked;
}

async function getUniqueFileHandle(dirHandle, initialName) {
  let name = sanitizeFileNameForFs(initialName);
  let count = 1;

  while (true) {
    try {
      await dirHandle.getFileHandle(name, { create: false });
      name = appendSuffixToName(initialName, `_${count}`);
      count += 1;
    } catch (_notFoundErr) {
      const fileHandle = await dirHandle.getFileHandle(name, { create: true });
      return { fileHandle, finalName: name };
    }
  }
}

async function writeMediaFileByFsApi(notebookPath, fileBlob, mime, originalName) {
  const mediaMime = String(mime || "").toLowerCase();
  const kind = mediaKindFromMime(mediaMime);
  if (!(kind === "image" || kind === "video")) {
    throw new Error("Unsupported media type");
  }

  const root = await ensureAuthorizedMediaRootHandle();
  const assetsDir = await root.getDirectoryHandle("assets", { create: true });
  const kindDirName = kind === "video" ? "video" : "image";
  const targetDir = await assetsDir.getDirectoryHandle(kindDirName, { create: true });

  const fallbackName = defaultNameByMime(mediaMime);
  const fallbackExt = splitFileName(fallbackName, "").ext || "";
  const stampedName = buildTimestampedFileName(originalName || fallbackName, fallbackExt);
  const unique = await getUniqueFileHandle(targetDir, stampedName);

  const writable = await unique.fileHandle.createWritable();
  try {
    await writable.write(fileBlob);
  } finally {
    await writable.close();
  }

  const relPath = `assets/${kindDirName}/${unique.finalName}`;
  const probe = await callEntry(
    "load_notebook_media_asset_payload",
    {
      notebook_path: String(notebookPath || state.currentPath || ""),
      asset_rel_path: relPath,
    },
    applyLargeNotebookAwareOptions({
      maxWaitMs: 120000,
      pollIntervalMs: 120,
      disableWorker: true,
    })
  );

  if (!probe?.success) {
    throw new Error(String(probe?.error || "asset verification failed"));
  }

  return {
    notebook_path: String(notebookPath || state.currentPath || ""),
    asset_kind: kind,
    asset_rel_path: relPath,
    asset_src: `./${relPath}`,
    media_mime: mediaMime,
    size_bytes: Math.max(0, Number(fileBlob?.size || 0)),
    original_name: String(originalName || fallbackName || unique.finalName),
    saved_name: String(unique.finalName || ""),
    html: kind === "video"
      ? `<video controls preload="metadata" src="./${relPath}"></video>`
      : `<img src="./${relPath}" alt="${String(originalName || unique.finalName || "image")}" />`,
  };
}

function buildUploadFsPickerOptions() {
  return {
    multiple: true,
    excludeAcceptAllOption: false,
    types: [
      {
        description: "图片文件",
        accept: {
          "image/*": UPLOAD_IMAGE_EXT_LIST,
        },
      },
      {
        description: "视频文件",
        accept: {
          "video/*": UPLOAD_VIDEO_EXT_LIST,
        },
      },
    ],
  };
}

async function requestUploadRootDirHandleInClickFlow() {
  if (!supportsFsAccessApi()) {
    throw new Error("当前浏览器不支持目录授权 API");
  }

  showToast("请选择你的ipynb笔记所在的根目录，授权读写权限", "info");
  const picked = await window.showDirectoryPicker({ mode: "readwrite" });
  if (!picked || typeof picked.getDirectoryHandle !== "function") {
    throw new Error("目录句柄无效");
  }

  let perm = "prompt";
  try {
    perm = await picked.queryPermission({ mode: "readwrite" });
  } catch (_err) {
    perm = "prompt";
  }
  if (perm !== "granted") {
    perm = await picked.requestPermission({ mode: "readwrite" });
  }
  if (perm !== "granted") {
    throw new Error("目录读写权限未授予");
  }
  return picked;
}

async function pickUploadFilesAfterDirAuth() {
  if (typeof window.showOpenFilePicker === "function") {
    const handles = await window.showOpenFilePicker(buildUploadFsPickerOptions());
    const files = [];
    for (const handle of (Array.isArray(handles) ? handles : [])) {
      if (!handle || typeof handle.getFile !== "function") continue;
      const file = await handle.getFile();
      if (file instanceof File) {
        files.push(file);
      }
    }
    return files;
  }

  return pickUploadFiles();
}

async function writeMediaFileByFsApiWithRoot(rootDirHandle, notebookPath, fileObj, mime, originalName) {
  if (!rootDirHandle || typeof rootDirHandle.getDirectoryHandle !== "function") {
    throw new Error("rootDirHandle 无效，无法写入文件");
  }

  const mediaMime = String(mime || "").toLowerCase();
  const kind = mediaKindFromMime(mediaMime);
  if (!(kind === "image" || kind === "video")) {
    throw new Error("不支持的媒体类型");
  }

  const kindDirName = kind === "video" ? "video" : "image";
  const fallbackName = defaultNameByMime(mediaMime);
  const fallbackExt = splitFileName(fallbackName, "").ext || "";
  const notebookPathText = String(notebookPath || state.currentPath || "");

  const assetsDir = await rootDirHandle.getDirectoryHandle("assets", { create: true });
  if (!assetsDir || typeof assetsDir.getDirectoryHandle !== "function") {
    throw new Error("assets 目录句柄无效");
  }

  const targetDir = await assetsDir.getDirectoryHandle(kindDirName, { create: true });
  if (!targetDir || typeof targetDir.getFileHandle !== "function") {
    throw new Error(`${kindDirName} 目录句柄无效`);
  }

  const stampedName = buildTimestampedFileName(originalName || fallbackName, fallbackExt);
  const unique = await getUniqueFileHandle(targetDir, stampedName);
  if (!unique || !unique.fileHandle || typeof unique.fileHandle.createWritable !== "function") {
    throw new Error("文件句柄创建失败");
  }

  const writable = await unique.fileHandle.createWritable();
  if (!writable || typeof writable.write !== "function" || typeof writable.close !== "function") {
    throw new Error("文件写入流创建失败");
  }

  try {
    await writable.write(fileObj);
  } finally {
    await writable.close();
  }

  const relPath = `assets/${kindDirName}/${unique.finalName}`;
  const probe = await callEntry(
    "load_notebook_media_asset_payload",
    {
      notebook_path: notebookPathText,
      asset_rel_path: relPath,
    },
    applyLargeNotebookAwareOptions({
      maxWaitMs: 120000,
      pollIntervalMs: 120,
      disableWorker: true,
    })
  );

  if (!probe?.success) {
    try {
      await targetDir.removeEntry(unique.finalName);
    } catch (_cleanupErr) {
      // Ignore cleanup errors after failed verification.
    }
    throw new Error(String(probe?.error || "上传后媒体校验失败"));
  }

  return {
    notebook_path: notebookPathText,
    asset_kind: kind,
    asset_rel_path: relPath,
    asset_src: `./${relPath}`,
    media_mime: mediaMime,
    size_bytes: Math.max(0, Number(fileObj?.size || 0)),
    original_name: String(originalName || fallbackName || unique.finalName),
    saved_name: String(unique.finalName || ""),
    html: kind === "video"
      ? `<video controls preload="metadata" src="./${relPath}"></video>`
      : `<img src="./${relPath}" alt="${String(originalName || unique.finalName || "image")}" />`,
  };
}

async function uploadMediaAssetRecordsForNotebook(notebookPath) {
  let files = [];

  try {
    setUiBusy("请选择要上传的图片/视频文件");
    files = await pickUploadFiles();
  } catch (err) {
    console.error("[Upload] 文件选择失败", err);
    throw new Error("文件选择失败，请重试");
  }

  if (!files.length) {
    return { records: [], failures: [] };
  }

  const records = [];
  const failures = [];
  const notebookPathText = String(notebookPath || state.currentPath || "");

  for (let i = 0; i < files.length; i += 1) {
    const file = files[i];
    const fileName = String(file?.name || `upload-${i + 1}`);
    if (!isSupportedUploadFile(file)) {
      failures.push(`${fileName}: 不支持的文件格式`);
      continue;
    }

    const mime = normalizeUploadMime(file);
    if (!(UPLOAD_IMAGE_MIME_LIST.includes(mime) || UPLOAD_VIDEO_MIME_LIST.includes(mime))) {
      failures.push(`${fileName}: 无法识别文件格式`);
      continue;
    }

    setUiBusy(`正在上传 ${i + 1}/${files.length}: ${fileName} (读取中 0%)`);
    try {
      const base64 = await fileToBase64WithProgress(file, (pct) => {
        setUiBusy(`正在上传 ${i + 1}/${files.length}: ${fileName} (读取中 ${pct}%)`);
      });
      setUiBusy(`正在上传 ${i + 1}/${files.length}: ${fileName} (写入 assets/${mediaKindFromMime(mime) || "image"})`);
      const record = await persistMediaAssetForNotebook(notebookPathText, mime, base64, fileName);
      records.push(record);
    } catch (err) {
      console.error(`[Upload] 资产落盘失败: ${fileName}`, err);
      failures.push(`${fileName}: ${String(err)}`);
    }

    await yieldToMainThread();
  }

  return { records, failures };
}

function buildViewAssetReferenceFromRecord(record) {
  const item = record && typeof record === "object" ? record : {};
  const relRaw = String(item.asset_rel_path || "").trim().replace(/\\/g, "/").replace(/^\.?\/+/, "");
  if (relRaw) {
    if (/^assets\//i.test(relRaw)) {
      return relRaw;
    }
    if (/^(image|video)\//i.test(relRaw)) {
      return `assets/${relRaw}`;
    }
  }

  const kind = String(item.asset_kind || mediaKindFromMime(item.media_mime) || "").toLowerCase();
  const savedName = String(item.saved_name || item.original_name || "").trim();
  if (savedName && (kind === "image" || kind === "video")) {
    return `assets/${kind}/${savedName}`;
  }
  return savedName;
}

function mergeViewSourceWithUploadedRecords(currentSource, records) {
  const rawLines = String(currentSource || "").split(/\r\n|\r|\n/);
  const lines = rawLines.map((line) => String(line || "").trim()).filter(Boolean);
  const seen = new Set(lines.map((line) => line.toLowerCase()));

  for (const item of (Array.isArray(records) ? records : [])) {
    const refText = String(buildViewAssetReferenceFromRecord(item) || "").trim();
    if (!refText) continue;
    const key = refText.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    lines.push(refText);
  }

  return lines.join("\n");
}

async function uploadFilesToViewCellSource(cellId) {
  if (!state.notebook || !Array.isArray(state.notebook.cells)) return;
  const targetId = String(cellId || state.selectedCellId || "");
  const cell = state.notebook.cells.find((item) => String(item.id) === targetId);
  if (!cell) {
    showToast("未找到目标单元格", "error");
    return;
  }
  if (normalizeUiCellType(cell.cell_type) !== "view") {
    showToast("请在视图单元格中上传文件", "info");
    return;
  }

  try {
    const { records, failures } = await uploadMediaAssetRecordsForNotebook(state.currentPath);
    if (!records.length) {
      if (failures.length) {
        showToast(failures.join("；"), "error");
      }
      return;
    }

    cell.source = mergeViewSourceWithUploadedRecords(cell.source, records);
    cell.details_loaded = true;
    cell.outline_only = false;
    cell.source_line_count = Math.max(1, String(cell.source || "").split(/\r\n|\r|\n/).length);
    touchCellActivity(cell.id);

    const ok = await persistNotebookStructure(
      true,
      applyLargeNotebookAwareOptions({
        maxWaitMs: 300000,
        pollIntervalMs: 120,
        disableWorker: true,
      })
    );
    if (!ok) {
      showToast("上传后保存失败", "error");
      return;
    }

    renderCurrentNotebook(true);
    if (failures.length) {
      showToast(`上传完成：成功 ${records.length}，失败 ${failures.length}`, "info");
    } else {
      showToast(`上传完成：成功 ${records.length}`, "success");
    }
  } catch (err) {
    showToast(String(err || "上传失败"), "error");
  } finally {
    setUiBusy("");
  }
}

async function uploadFilesToCellOutput(cellId) {
  if (!state.notebook || !Array.isArray(state.notebook.cells)) return;
  const targetId = String(cellId || state.selectedCellId || "");
  const cell = state.notebook.cells.find((item) => String(item.id) === targetId);
  if (!cell) {
    showToast("未找到目标单元格", "error");
    return;
  }
  if (cell.cell_type !== "code") {
    showToast("请在代码单元格中上传文件", "info");
    return;
  }

  try {
    const { records, failures } = await uploadMediaAssetRecordsForNotebook(state.currentPath);
    const outputs = records.map((record) => mediaOutputFromAssetRecord(record));

    if (!outputs.length) {
      const msg = failures.length ? failures.join("；") : "上传失败";
      showToast(msg, "error");
      return;
    }

    cell.outputs = Array.isArray(cell.outputs) ? cell.outputs : [];
    cell.outputs.push(...outputs);
    cell.output_text = "";

    const ok = await persistNotebookStructure(
      true,
      applyLargeNotebookAwareOptions({
        maxWaitMs: 300000,
        pollIntervalMs: 120,
        disableWorker: true,
      })
    );
    if (!ok) {
      showToast("上传后保存失败", "error");
      return;
    }

    renderCurrentNotebook(true);
    if (failures.length) {
      showToast(`上传完成：成功 ${outputs.length}，失败 ${failures.length}`, "info");
    } else {
      showToast(`上传完成：成功 ${outputs.length}`, "success");
    }
  } finally {
    setUiBusy("");
  }
}

function mediaOutputFromAssetRecord(record) {
  const item = record && typeof record === "object" ? record : {};
  const mime = String(item.media_mime || "application/octet-stream").toLowerCase();
  const relPath = String(item.asset_rel_path || "");
  const html = String(item.html || "");
  const kind = String(item.asset_kind || mediaKindFromMime(mime) || "");
  return {
    output_type: "display_data",
    data: {
      "text/html": html,
    },
    metadata: {
      neko_media_mime: mime,
      neko_media_kind: kind,
      neko_media_size_bytes: Math.max(0, Number(item.size_bytes || 0)),
      neko_media_asset_path: relPath,
      neko_media_asset_src: String(item.asset_src || ""),
      neko_media_original_name: String(item.original_name || ""),
      neko_media_saved_name: String(item.saved_name || ""),
      neko_notebook_path: String(item.notebook_path || state.currentPath || ""),
    },
  };
}

async function persistMediaAssetForNotebook(notebookPath, mime, base64, originalName) {
  const resp = await callEntry(
    "store_notebook_media_asset",
    {
      notebook_path: String(notebookPath || state.currentPath || ""),
      media_mime: String(mime || "").toLowerCase(),
      base64: normalizeBase64Text(base64),
      original_name: String(originalName || ""),
    },
    applyLargeNotebookAwareOptions({
      maxWaitMs: 180000,
      pollIntervalMs: 220,
      priority: 78,
    })
  );

  if (!resp?.success) {
    throw new Error(String(resp?.error || "保存媒体文件失败"));
  }
  return resp.data && typeof resp.data === "object" ? resp.data : {};
}

async function persistMediaAssetForNotebookFromPath(notebookPath, sourcePath, mime = "", originalName = "") {
  const resp = await callEntry(
    "store_notebook_media_asset_from_path",
    {
      notebook_path: String(notebookPath || state.currentPath || ""),
      source_path: String(sourcePath || ""),
      media_mime: String(mime || "").toLowerCase(),
      original_name: String(originalName || ""),
    },
    applyLargeNotebookAwareOptions({
      maxWaitMs: 180000,
      pollIntervalMs: 160,
      priority: 82,
    })
  );

  if (!resp?.success) {
    throw new Error(String(resp?.error || "按路径保存媒体文件失败"));
  }
  return resp.data && typeof resp.data === "object" ? resp.data : {};
}

function mediaOutputFromBase64(mime, base64, options = {}) {
  const normalizedMime = String(mime || "application/octet-stream").toLowerCase();
  const normalizedBase64 = normalizeBase64Text(base64);
  const sizeBytes = estimateBase64Bytes(normalizedBase64);

  if (options && options.cacheOnly === true) {
    const token = cacheMediaPayload(normalizedMime, normalizedBase64, sizeBytes);
    return {
      output_type: "display_data",
      data: {},
      metadata: {
        neko_media_size_bytes: sizeBytes,
        neko_media_token: token,
        neko_media_mime: normalizedMime,
      },
    };
  }

  return {
    output_type: "display_data",
    data: {
      [normalizedMime]: normalizedBase64,
    },
    metadata: {
      neko_media_size_bytes: sizeBytes,
      neko_media_mime: normalizedMime,
    },
  };
}

function videoOutputFromDataUrl(mime, dataUrl, options = {}) {
  const marker = ",";
  const at = String(dataUrl || "").indexOf(marker);
  const base64 = at >= 0 ? String(dataUrl).slice(at + 1) : "";
  return mediaOutputFromBase64(mime, base64, options);
}

function materializeOutputForPersist(outputItem) {
  const out = outputItem && typeof outputItem === "object" ? outputItem : {};
  const cloned = {
    ...out,
    data: out.data && typeof out.data === "object" ? { ...out.data } : {},
    metadata: out.metadata && typeof out.metadata === "object" ? { ...out.metadata } : {},
  };

  const meta = cloned.metadata;
  const token = String(meta.neko_media_token || "");
  const mime = String(meta.neko_media_mime || "").toLowerCase();
  if (token && mime) {
    const cached = getCachedMediaPayload(token);
    if (cached && cached.base64) {
      cloned.data[mime] = String(cached.base64 || "");
      delete cloned.metadata.neko_media_token;
    }
  }

  return cloned;
}

async function pasteClipboardMediaToCellOutput(cellId) {
  if (!state.notebook || !Array.isArray(state.notebook.cells)) return;
  const targetId = String(cellId || state.selectedCellId || "");
  const cell = state.notebook.cells.find((item) => String(item.id) === targetId);
  if (!cell) {
    showToast("未找到目标单元格", "error");
    return;
  }
  if (cell.cell_type !== "code") {
    showToast("请在代码单元格中粘贴图片", "info");
    return;
  }
  try {
    const outputs = [];
    const failures = [];
    const mediaCandidates = [];
    let fallbackOnly = false;
    const clipboardReadable = Boolean(navigator.clipboard && typeof navigator.clipboard.read === "function");

    if (clipboardReadable) {
      try {
        const clipboardItems = await navigator.clipboard.read();

        for (const item of clipboardItems) {
          const types = Array.isArray(item.types) ? item.types : [];
          for (const type of types) {
            const blob = await item.getType(type);
            const blobMime = String(blob?.type || type || "").toLowerCase();

            if (blobMime.startsWith("image/")) {
              const dataUrl = await blobToDataUrl(blob);
              const marker = ",";
              const at = String(dataUrl || "").indexOf(marker);
              const base64 = at >= 0 ? String(dataUrl).slice(at + 1) : "";
              mediaCandidates.push({
                mime: blobMime,
                base64,
                originalName: String(blob?.name || defaultNameByMime(blobMime)),
              });
            }
          }
        }
      } catch (_err) {
        // Clipboard read permission/runtime failures should not block backend fallback.
      }
    }

    // Browser clipboard API often cannot directly expose Explorer-copied files on Windows.
    // Fallback to backend clipboard file reader for image paste.
    if (!mediaCandidates.length) {
      const backendResp = await callEntry("read_clipboard_media_files", { max_files: 8 });
      if (backendResp.success) {
        const mediaItems = Array.isArray(backendResp.data?.media_items) ? backendResp.data.media_items : [];
        for (const item of mediaItems) {
          if (!item || typeof item !== "object") continue;
          const mime = String(item.mime || "").toLowerCase();
          if (!mime.startsWith("image/")) continue;
          const base64 = String(item.base64 || "");
          if (!base64 || !mime) continue;
          mediaCandidates.push({
            mime,
            base64,
            originalName: String(item.name || defaultNameByMime(mime)),
            sourcePath: String(item.path || ""),
          });
        }
      }
    }

    if (!mediaCandidates.length) {
      showToast("剪贴板中未检测到图片", "info");
      return;
    }

    for (const candidate of mediaCandidates) {
      const mime = String(candidate?.mime || "").toLowerCase();
      if (!mime.startsWith("image/")) continue;
      const base64 = normalizeBase64Text(candidate?.base64 || "");
      const sourcePath = String(candidate?.sourcePath || "");
      const originalName = String(candidate?.originalName || defaultNameByMime(mime));
      if (!mime || !base64) continue;

      let storedByFs = false;
      if (!fallbackOnly && supportsFsAccessApi()) {
        try {
          setUiBusy(`正在写入图片到 assets/image: ${originalName}`);
          const fileBlob = await decodeBase64ToBlobChunked(base64, mime);
          const fsRecord = await writeMediaFileByFsApi(state.currentPath, fileBlob, mime, originalName);
          outputs.push(mediaOutputFromAssetRecord(fsRecord));
          storedByFs = true;
        } catch (err) {
          if (isFsAccessDeniedError(err)) {
            fallbackOnly = true;
            showToast("目录授权被拒绝，已自动回退为后端落盘模式", "info");
          }
        }
      }

      if (storedByFs) {
        continue;
      }

      try {
        setUiBusy(`正在写入图片到 assets/image: ${originalName}`);
        let record = null;
        if (sourcePath) {
          record = await persistMediaAssetForNotebookFromPath(state.currentPath, sourcePath, mime, originalName);
        } else {
          record = await persistMediaAssetForNotebook(state.currentPath, mime, base64, originalName);
        }
        outputs.push(mediaOutputFromAssetRecord(record));
      } catch (err) {
        console.error(`[Paste] 资产落盘失败: ${originalName}`, err);
        failures.push(`${originalName}: ${String(err)}`);
      }
    }

    if (!outputs.length) {
      showToast("剪贴板中未检测到可用图片", "info");
      return;
    }

    cell.outputs = Array.isArray(cell.outputs) ? cell.outputs : [];
    cell.outputs.push(...outputs);
    cell.output_text = "";

    const ok = await persistNotebookStructure(true);
    if (!ok) {
      showToast("粘贴失败", "error");
      return;
    }

    renderCurrentNotebook(true);
    if (failures.length) {
      showToast(`已粘贴 ${outputs.length} 张图片，失败 ${failures.length} 张`, "info");
    } else {
      showToast(`已粘贴 ${outputs.length} 张图片`, "success");
    }
  } catch (err) {
    showToast(`粘贴失败: ${String(err)}`, "error");
  } finally {
    setUiBusy("");
  }
}

async function deleteCell(cellId, options = {}) {
  if (!state.currentPath) return;
  const needConfirm = options.confirm !== false;
  if (needConfirm && !window.confirm("确定删除该单元格吗？")) return;

  try {
    setUiBusy("正在删除单元格...");
    const targetIndex = Array.isArray(state.notebook?.cells)
      ? state.notebook.cells.findIndex((cell) => String(cell.id) === String(cellId))
      : -1;

    const resp = await callEntry(
      "delete_cell",
      {
        notebook_path: state.currentPath,
        cell_id: cellId,
        cell_index: targetIndex,
        include_notebook: false,
      },
      applyLargeNotebookAwareOptions({
        maxWaitMs: 60000,
        pollIntervalMs: 200,
      })
    );

    if (!resp.success) {
      showToast(resp.error || "删除失败", "error");
      return;
    }

    const hadNotebookPayload = Boolean(resp.data?.notebook && typeof resp.data.notebook === "object");
    if (hadNotebookPayload) {
      state.notebook = resp.data.notebook || state.notebook;
      state.notebook.cells = (state.notebook.cells || []).map((cell, index) => normalizeCell(cell, index));
    } else if (targetIndex >= 0 && Array.isArray(state.notebook?.cells)) {
      state.notebook.cells.splice(targetIndex, 1);
    }

    delete state.cellStatus[cellId];
    state.collapsedCellIds.delete(String(cellId));
    state.outputExpandedCellIds.delete(String(cellId));
    state.cellLastActiveAt.delete(String(cellId));
    state.cellDetailHydrationPromises.delete(String(cellId));

    const fallbackIndex = Math.max(0, Math.min(targetIndex, (state.notebook?.cells?.length || 1) - 1));
    const fallback = state.notebook?.cells?.[fallbackIndex] || state.notebook?.cells?.[0] || null;
    state.selectedCellId = fallback ? String(fallback.id) : "";

    if (hadNotebookPayload || useVirtualRender(state.notebook?.cells || [])) {
      renderCurrentNotebook(true);
    } else {
      const node = document.querySelector(`.jp-cell[data-cell-id="${String(cellId)}"]`);
      if (node instanceof HTMLElement) {
        node.remove();
      } else {
        renderCurrentNotebook(true);
      }
      if (targetIndex >= 0) {
        refreshVisibleCellIndices(targetIndex + 1);
      }
    }

    renderRunningList();
    scheduleNotebookListRefresh(320);
    showToast("删除成功", "success");
  } catch (err) {
    showToast(`删除失败: ${String(err)}`, "error");
  } finally {
    setUiBusy("");
  }
}

async function deleteSelectedCell() {
  const selected = ensureSelectedCell();
  if (!selected) {
    showToast("没有可删除的单元格", "info");
    return;
  }
  await deleteCell(selected.id);
}

async function insertCellRelative(cellId, where) {
  if (!state.notebook || !Array.isArray(state.notebook.cells)) return;
  const id = String(cellId || "");
  const index = state.notebook.cells.findIndex((cell) => String(cell.id) === id);
  if (index < 0) {
    showToast("未找到目标单元格", "error");
    return;
  }
  const insertIndex = String(where) === "above" ? index : index + 1;
  setSelectedCell(id);
  await addCell("code", { index: insertIndex });
}

async function moveCellById(cellId, delta) {
  const id = String(cellId || "");
  if (!state.notebook || !Array.isArray(state.notebook.cells)) {
    showToast("请先打开笔记本", "error");
    return;
  }

  const cells = state.notebook.cells;
  const index = cells.findIndex((cell) => String(cell.id) === id);
  const target = index + Number(delta || 0);
  if (index < 0 || target < 0 || target >= cells.length) {
    showToast("无法继续移动", "info");
    return;
  }

  const current = cells[index];
  cells[index] = cells[target];
  cells[target] = current;

  const ok = await persistNotebookStructure(true);
  if (!ok) {
    showToast("移动失败", "error");
    return;
  }

  state.selectedCellId = String(cells[target].id);
  renderCurrentNotebook(true);
  showToast("移动成功", "success");
}

async function runCell(cellId, triggerOptions = {}) {
  if (!state.currentPath) return;
  const runTrace = {
    cellId: String(cellId || ""),
    trigger: String(triggerOptions?.trigger || "unknown"),
    clickAt: Number(triggerOptions?.clickAt || nowPerfMs()),
    eventTriggeredAt: nowPerfMs(),
  };

  const targetIndex = Array.isArray(state.notebook?.cells)
    ? state.notebook.cells.findIndex((cell) => String(cell.id) === String(cellId))
    : -1;
  const targetCell = targetIndex >= 0 ? state.notebook.cells[targetIndex] : null;

  if (updateTimers.has(String(cellId))) {
    clearTimeout(updateTimers.get(String(cellId)));
    updateTimers.delete(String(cellId));
  }

  state.cellStatus[cellId] = "running";
  touchCellActivity(cellId);
  setUiBusy("正在执行单元格...");
  setSelectedCell(cellId);
  patchCellStatusBadge(cellId);
  renderRunningList();

  try {
    const latestCell = (Array.isArray(state.notebook?.cells) && targetIndex >= 0) ? state.notebook.cells[targetIndex] : targetCell;
    const useBackendStoredSource = Boolean(latestCell && latestCell.details_loaded === false);
    const sourceOverride = useBackendStoredSource ? "" : (latestCell ? String(latestCell.source || "") : "");
    const timeoutSec = Number(state.uiInfo?.execution_timeout_sec || 90);
    const effectiveTimeoutSec = state.largeNotebookMode ? Math.max(timeoutSec, 150) : timeoutSec;
    runTrace.dispatchStartedAt = nowPerfMs();
    const resp = await callEntry(
      "execute_cell",
      {
        notebook_path: state.currentPath,
        cell_id: cellId,
        cell_index: targetIndex,
        source: sourceOverride,
        timeout_sec: effectiveTimeoutSec,
        include_notebook: false,
      },
      {
        maxWaitMs: Math.max(180000, (Math.max(5, effectiveTimeoutSec) + 90) * 1000),
        pollIntervalMs: RUN_FAST_LANE_POLL_INTERVAL_MS,
        disableWorker: true,
        forceFastLane: true,
        _perf: runTrace,
      }
    );
    runTrace.responseReceivedAt = nowPerfMs();

    if (!resp.success) {
      state.cellStatus[cellId] = "failed";
      patchCellStatusBadge(cellId);
      patchSingleCellDom(cellId, { keepScroll: true });
      runTrace.renderCompletedAt = nowPerfMs();
      emitRunLatencyTrace(runTrace, resp?.data?.perf_trace, { success: false });
      renderRunningList();
      showToast("执行失败", "error");
      return;
    }

    if (resp.data?.notebook && typeof resp.data.notebook === "object") {
      state.notebook = resp.data.notebook || state.notebook;
      state.notebook.cells = (state.notebook.cells || []).map((cell, index) => normalizeCell(cell, index));
    } else if (resp.data?.cell && typeof resp.data.cell === "object") {
      const idx = Array.isArray(state.notebook?.cells)
        ? state.notebook.cells.findIndex((cell) => String(cell.id) === String(cellId))
        : -1;
      if (idx >= 0) {
        state.notebook.cells[idx] = normalizeCell(resp.data.cell, idx);
      }
    }
    state.cellStatus[cellId] = resp.data.success ? "success" : "failed";
    patchCellStatusBadge(cellId);
    patchSingleCellDom(cellId, { keepScroll: true });
    runTrace.renderCompletedAt = nowPerfMs();
    emitRunLatencyTrace(runTrace, resp?.data?.perf_trace, { success: Boolean(resp.data.success) });
    renderRunningList();
    scheduleNotebookListRefresh(260);
    showToast(resp.data.success ? "执行成功" : "执行失败", resp.data.success ? "success" : "error");
  } catch (_err) {
    state.cellStatus[cellId] = "failed";
    patchCellStatusBadge(cellId);
    patchSingleCellDom(cellId, { keepScroll: true });
    runTrace.responseReceivedAt = nowPerfMs();
    runTrace.renderCompletedAt = nowPerfMs();
    emitRunLatencyTrace(runTrace, {}, { success: false });
    renderRunningList();
    showToast("执行失败", "error");
  } finally {
    setUiBusy("");
  }
}

function getCellById(cellId) {
  const key = String(cellId || "");
  if (!key || !Array.isArray(state.notebook?.cells)) {
    return null;
  }
  return state.notebook.cells.find((cell) => String(cell.id) === key) || null;
}

function focusCellSourceEditor(cellId) {
  const key = String(cellId || "");
  if (!key) return;

  const focusOnce = () => {
    const source = document.querySelector(`.jp-cell-source[data-cell-id="${key}"]`);
    if (!(source instanceof HTMLTextAreaElement)) {
      return false;
    }
    source.focus();
    const len = source.value.length;
    source.setSelectionRange(len, len);
    return true;
  };

  if (!focusOnce()) {
    requestAnimationFrame(() => {
      focusOnce();
    });
  }
}

async function runCodeCellByShortcut(cellId, mode) {
  const key = String(cellId || "");
  if (!key) return;
  if (runShortcutBusy) return;

  const currentCell = getCellById(key);
  if (!currentCell || String(currentCell.cell_type) !== "code") {
    return;
  }

  runShortcutBusy = true;
  try {
    setSelectedCell(key);
    await flushPendingCellUpdate(key);
    await runCell(key, {
      clickAt: nowPerfMs(),
      trigger: `shortcut_${String(mode || "advance")}`,
    });

    const cells = Array.isArray(state.notebook?.cells) ? state.notebook.cells : [];
    const currentIndex = cells.findIndex((cell) => String(cell.id) === key);
    if (currentIndex < 0) {
      return;
    }

    if (mode === "stay") {
      focusCellSourceEditor(key);
      return;
    }

    if (mode === "insert-below") {
      const createdId = await addCell("code", { index: currentIndex + 1, silent: true });
      const targetId = String(createdId || state.selectedCellId || "");
      if (targetId) {
        setSelectedCell(targetId);
        focusCellSourceEditor(targetId);
      }
      return;
    }

    const next = cells[currentIndex + 1];
    if (next) {
      const nextId = String(next.id || "");
      if (!nextId) return;
      state.collapsedCellIds.delete(nextId);
      setSelectedCell(nextId);
      patchSingleCellDom(nextId, { keepScroll: true });
      focusCellSourceEditor(nextId);
      return;
    }

    const createdId = await addCell("code", { index: currentIndex + 1, silent: true });
    const targetId = String(createdId || state.selectedCellId || "");
    if (targetId) {
      setSelectedCell(targetId);
      focusCellSourceEditor(targetId);
    }
  } catch (err) {
    console.error("[Shortcut] 运行快捷键执行失败", err);
  } finally {
    runShortcutBusy = false;
  }
}

async function handleNotebookEditorRunShortcut(event) {
  const target = event.target;
  if (!(target instanceof HTMLTextAreaElement)) {
    return;
  }
  if (!target.classList.contains("jp-cell-source")) {
    return;
  }

  const cellId = String(target.dataset.cellId || "");
  if (!cellId) {
    return;
  }

  const cell = getCellById(cellId);
  if (!cell || String(cell.cell_type) !== "code") {
    return;
  }

  const mode = event.altKey
    ? "insert-below"
    : ((event.ctrlKey || event.metaKey) ? "stay" : "advance");

  event.preventDefault();
  event.stopPropagation();
  await runCodeCellByShortcut(cellId, mode);
}

async function runSelectedCell() {
  const selected = ensureSelectedCell();
  if (!selected) {
    showToast("请先选中单元格", "info");
    return;
  }
  if (selected.cell_type !== "code") {
    showToast("仅代码单元格支持运行", "info");
    return;
  }
  await runCell(selected.id, {
    clickAt: nowPerfMs(),
    trigger: "run_selected",
  });
}

async function runAllCodeCells() {
  if (!state.notebook || !Array.isArray(state.notebook.cells)) {
    showToast("请先打开笔记本", "error");
    return;
  }

  await ensureNotebookFullyLoaded();

  const codeCells = state.notebook.cells.filter((cell) => cell.cell_type === "code");
  if (!codeCells.length) {
    showToast("当前笔记本没有代码单元格", "info");
    return;
  }

  for (const cell of codeCells) {
    await runCell(cell.id, {
      clickAt: nowPerfMs(),
      trigger: "run_all",
    });
  }
}

async function clearAllOutputs() {
  if (!state.notebook || !Array.isArray(state.notebook.cells) || !state.notebook.cells.length) {
    showToast("当前没有可清空的输出", "info");
    return;
  }

  state.notebook.cells.forEach((cell) => {
    if (cell.cell_type === "code") {
      cell.outputs = [];
      cell.output_text = "";
      cell.execution_count = null;
      delete state.cellStatus[cell.id];
    }
  });

  const ok = await persistNotebookStructure(true);
  if (!ok) {
    showToast("清空输出失败", "error");
    return;
  }

  renderCurrentNotebook(true);
  renderRunningList();
  showToast("输出已清空", "success");
}

async function moveSelectedCell(delta) {
  const selected = ensureSelectedCell();
  if (!selected) {
    showToast("请先选中单元格", "info");
    return;
  }
  await moveCellById(selected.id, delta);
}

async function mergeSelectedWithNext() {
  const selected = ensureSelectedCell();
  if (!selected) {
    showToast("请先选中单元格", "info");
    return;
  }

  const cells = state.notebook.cells;
  const index = getSelectedCellIndex();
  if (index < 0 || index >= cells.length - 1) {
    showToast("下方没有可合并单元格", "info");
    return;
  }

  const next = cells[index + 1];
  if (selected.cell_type !== next.cell_type) {
    showToast("仅支持同类型单元格合并", "info");
    return;
  }

  const left = String(selected.source || "");
  const right = String(next.source || "");
  selected.source = `${left}${left && right ? "\n" : ""}${right}`;

  if (selected.cell_type === "code") {
    selected.outputs = [];
    selected.output_text = "";
    selected.execution_count = null;
  }

  cells.splice(index + 1, 1);
  state.collapsedCellIds.delete(String(next.id));

  const ok = await persistNotebookStructure(true);
  if (!ok) {
    showToast("合并失败", "error");
    return;
  }

  state.selectedCellId = String(selected.id);
  renderCurrentNotebook(true);
  showToast("已合并单元格", "success");
}

function getActiveTextareaCursor(cellId, sourceText) {
  const active = document.activeElement;
  if (
    active instanceof HTMLTextAreaElement
    && active.classList.contains("jp-cell-source")
    && String(active.dataset.cellId || "") === String(cellId)
  ) {
    const pos = Number(active.selectionStart || 0);
    if (Number.isFinite(pos) && pos >= 0) {
      return Math.min(pos, sourceText.length);
    }
  }
  return Math.floor(sourceText.length / 2);
}

async function splitSelectedAtCursor() {
  const selected = ensureSelectedCell();
  if (!selected) {
    showToast("请先选中单元格", "info");
    return;
  }

  const sourceText = String(selected.source || "");
  if (!sourceText.length) {
    showToast("当前单元格内容为空，无法拆分", "info");
    return;
  }

  const index = getSelectedCellIndex();
  if (index < 0) return;

  const cursor = getActiveTextareaCursor(selected.id, sourceText);
  if (cursor <= 0 || cursor >= sourceText.length) {
    showToast("请将光标放到中间位置再拆分", "info");
    return;
  }

  const left = sourceText.slice(0, cursor);
  const right = sourceText.slice(cursor);

  selected.source = left;
  if (selected.cell_type === "code") {
    selected.outputs = [];
    selected.output_text = "";
    selected.execution_count = null;
  }

  const newCell = createLocalCell(selected.cell_type, right);
  state.notebook.cells.splice(index + 1, 0, newCell);

  const ok = await persistNotebookStructure(true);
  if (!ok) {
    showToast("拆分失败", "error");
    return;
  }

  const fresh = state.notebook.cells[index + 1];
  state.selectedCellId = fresh ? String(fresh.id) : state.selectedCellId;
  renderCurrentNotebook(true);
  showToast("已拆分单元格", "success");
}

async function findAndReplaceInNotebook() {
  if (!state.notebook || !Array.isArray(state.notebook.cells) || !state.notebook.cells.length) {
    showToast("当前没有可替换内容", "info");
    return;
  }

  const keyword = window.prompt("请输入要查找的文本");
  if (keyword == null || keyword === "") return;

  const replacement = window.prompt("请输入替换文本", "");
  if (replacement == null) return;

  let changed = 0;
  state.notebook.cells.forEach((cell) => {
    const source = String(cell.source || "");
    if (!source.includes(keyword)) return;
    const count = source.split(keyword).length - 1;
    if (count > 0) {
      cell.source = source.split(keyword).join(replacement);
      changed += count;
    }
  });

  if (!changed) {
    showToast("未找到匹配文本", "info");
    return;
  }

  const ok = await persistNotebookStructure(true);
  if (!ok) {
    showToast("替换失败", "error");
    return;
  }

  renderCurrentNotebook(true);
  showToast(`替换完成: ${changed} 处`, "success");
}

async function restartKernel() {
  if (!state.currentPath) {
    showToast("请先打开笔记本", "error");
    return;
  }

  try {
    setUiBusy("正在重启内核...");
    let resp = await callEntry(
      "restart_kernel",
      { notebook_path: state.currentPath },
      applyLargeNotebookAwareOptions({ maxWaitMs: 180000, pollIntervalMs: 100, disableWorker: true })
    );
    if (!resp.success) {
      await callEntry(
        "shutdown_kernel",
        { notebook_path: state.currentPath },
        applyLargeNotebookAwareOptions({ maxWaitMs: 120000, pollIntervalMs: 100, disableWorker: true })
      );
      resp = await callEntry(
        "restart_kernel",
        { notebook_path: state.currentPath },
        applyLargeNotebookAwareOptions({ maxWaitMs: 180000, pollIntervalMs: 100, disableWorker: true })
      );
    }
    if (!resp.success) {
      showToast(resp.error || "重启内核失败", "error");
      return;
    }
    showToast("内核已重启", "success");
  } catch (err) {
    showToast(`重启内核失败: ${String(err)}`, "error");
  } finally {
    setUiBusy("");
  }
}

function markdownToHtml(text) {
  const escaped = escapeHtml(String(text || ""));
  const withHeadings = escaped
    .replace(/^#### (.*)$/gm, "<h4>$1</h4>")
    .replace(/^### (.*)$/gm, "<h3>$1</h3>")
    .replace(/^## (.*)$/gm, "<h2>$1</h2>")
    .replace(/^# (.*)$/gm, "<h1>$1</h1>");

  const withBold = withHeadings.replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>");
  const withCode = withBold.replace(/`([^`]+)`/g, "<code>$1</code>");
  const withList = withCode.replace(/^\*\s+(.*)$/gm, "• $1");
  return withList.replace(/\n/g, "<br>");
}

function escapeHtml(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function init() {
  ensureNotebookWorker();
  loadFloatingPrefs();
  applyFloatingPrefs();
  bindEvents();
  switchSidebarTab("files");
  await loadUiInfo();
  await refreshNotebookList();

  if (state.notebooks.length > 0) {
    await openNotebook(state.notebooks[0].path);
  }

  if (window.innerWidth > 900) {
    toggleMobileToolbar(false);
  }

  setInterval(collectClientGarbage, 15000);
}

window.addEventListener("beforeunload", () => {
  for (const timer of updateTimers.values()) {
    clearTimeout(timer);
  }
  clearRunQueueTimer();
  runQueue.pending.clear();
  if (state.worker instanceof Worker) {
    try {
      state.worker.postMessage({ type: "clear" });
      setTimeout(() => {
        try {
          state.worker?.terminate();
        } catch (_err) {
          // Ignore delayed worker shutdown errors.
        }
      }, 80);
    } catch (_err) {
      // Ignore worker shutdown errors.
    }
  }
  state.worker = null;
  state.workerReady = false;
  state.cellDetailHydrationPromises.clear();
  state.cellLastActiveAt.clear();
  state.outputExpandedCellIds.clear();
  state.mediaPayloadCache.clear();
  revokeAllMediaObjectUrls();
  for (const pending of state.workerRequests.values()) {
    try {
      pending.reject(new Error("worker terminated"));
    } catch (_err) {
      // Ignore rejection race conditions.
    }
  }
  state.workerRequests.clear();
  if (notebookListRefreshTimer) {
    clearTimeout(notebookListRefreshTimer);
    notebookListRefreshTimer = null;
  }
  stopVoiceInput();
  if (state.screenRecorder && state.screenRecorder.state !== "inactive") {
    state.screenRecorder.stop();
  }
  stopRecordingStream();
});

init().catch((err) => {
  console.error(err);
  showToast(`初始化失败: ${String(err)}`, "error");
});
