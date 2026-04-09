"""
Jupyter Notebook integration plugin for N.E.K.O.

This plugin provides:
- notebook file management (create/load/save)
- code/markdown cell editing
- kernel-backed code execution (jupyter_client)
- plugin-local isolated runtime env (venv + auto dependency bootstrap)
- proactive realtime event push to main dialogue model
- static Web UI under /plugin/jupyter_notebook/ui/

Web UI quick access:
http://127.0.0.1:48916/plugin/jupyter_notebook/ui/
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import importlib
import json
import mimetypes
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from plugin.sdk.plugin import (
    Err,
    NekoPluginBase,
    Ok,
    SdkError,
    lifecycle,
    neko_plugin,
    plugin_entry,
)

try:
    import nbformat  # type: ignore
except Exception:  # pragma: no cover
    nbformat = None

try:
    from jupyter_client import AsyncKernelManager  # type: ignore
except Exception:  # pragma: no cover
    AsyncKernelManager = None


_DEFAULT_NOTEBOOK_NAME = "Untitled.ipynb"
_DEFAULT_KERNEL_NAME = "python3"
_DEFAULT_EXEC_TIMEOUT_SEC = 90.0
_CONTENT_PUSH_COOLDOWN_SEC = 2.0
_MAX_EVENT_QUEUE = 1000
_MAX_RECENT_EVENTS = 200
_DEFAULT_BOOTSTRAP_TIMEOUT_SEC = 420
_AI_PUSH_RETRY_COUNT = 2
_AI_PUSH_RETRY_DELAY_SEC = 0.2
_EVENT_PUSH_RETRY_COUNT = 3
_EVENT_PUSH_RETRY_DELAY_SEC = 0.12
_CLIPBOARD_MEDIA_MAX_BYTES = 80 * 1024 * 1024
_STREAM_OUTLINE_SCAN_PREFIX_CHARS = 320000
_LAZY_MEDIA_THRESHOLD_BYTES = 5 * 1024 * 1024
_INLINE_IMAGE_MAX_BYTES = 1 * 1024 * 1024
_ALLOWED_NOTEBOOK_CELL_TYPES = {"code", "markdown", "view"}
_VIEW_CELL_METADATA_KEY = "neko_cell_type"
_RUNTIME_REQUIREMENTS = [
    "pip>=24.0",
    "setuptools>=68.0",
    "wheel>=0.43.0",
    "jupyter_server>=2.4.0,<3",
    "jupyterlab>=4.6.0a4,<4.7",
    "jupyterlab_server>=2.28.0,<3",
    "notebook_shim>=0.2,<0.3",
    "tornado>=6.2.0",
    "jupyter_client>=8.6.0",
    "ipykernel>=6.29.0",
    "nbformat>=5.10.0",
    "ipython>=8.0.0",
    "ipywidgets>=8.0.0",
    "matplotlib>=3.8.0",
    "matplotlib-inline>=0.1.7",
    "numpy>=1.26.0",
    "pandas>=2.2.0",
    "seaborn>=0.13.2",
    "plotly>=6.0.0",
]

_MODEL_PUSH_BLOCKED_EVENT_TYPES = {
    "cell_execution_stream",
    "code_cell_updated",
    "markdown_cell_updated",
}


@dataclass
class KernelRuntime:
    manager: Any
    client: Any
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    last_used_monotonic: float = field(default_factory=time.monotonic)


@neko_plugin
class JupyterNotebookPlugin(NekoPluginBase):
    def __init__(self, ctx):
        super().__init__(ctx)
        self.file_logger = self.enable_file_logging(log_level="INFO")
        self.logger = self.file_logger

        self._workspace_root = self.data_path("workspace")
        self._notebook_lock = threading.RLock()

        self._event_queue: deque[dict[str, Any]] = deque()
        self._recent_events: deque[dict[str, Any]] = deque(maxlen=_MAX_RECENT_EVENTS)
        self._event_lock = threading.Lock()
        self._event_wake = threading.Event()
        self._event_stop = threading.Event()
        self._event_thread: threading.Thread | None = None

        self._kernel_lock = asyncio.Lock()
        self._kernels: dict[str, KernelRuntime] = {}

        self._runtime_root = self.data_path("runtime")
        self._venv_dir = self._runtime_root / "venv"
        self._venv_python: Path | None = None
        self._venv_python_exec: str = ""
        self._runtime_ready = False
        self._runtime_error = ""
        self._runtime_bootstrap_timeout_sec = _DEFAULT_BOOTSTRAP_TIMEOUT_SEC

        self._default_kernel_name = _DEFAULT_KERNEL_NAME
        self._default_exec_timeout_sec = _DEFAULT_EXEC_TIMEOUT_SEC
        self._push_priority = 6
        self._enable_content_push = False
        self._enable_model_push = False
        self._max_event_queue = _MAX_EVENT_QUEUE
        self._prewarm_kernel = True
        self._shared_kernel = True
        self._kernel_hot_loaded = False

        self._last_content_push_at: dict[str, float] = {}

    @lifecycle(id="startup")
    async def startup(self, **_):
        self._workspace_root.mkdir(parents=True, exist_ok=True)

        self._kernel_lock = asyncio.Lock()
        self._kernels = {}
        self._last_content_push_at = {}
        self._venv_python_exec = ""
        self._kernel_hot_loaded = False

        cfg_obj = await self.config.dump(timeout=5.0)
        cfg: dict[str, Any] = dict(cfg_obj) if isinstance(cfg_obj, dict) else {}
        plugin_cfg_obj = cfg.get("jupyter_notebook")
        plugin_cfg: dict[str, Any] = dict(plugin_cfg_obj) if isinstance(plugin_cfg_obj, dict) else {}

        self._default_kernel_name = str(plugin_cfg.get("kernel_name", _DEFAULT_KERNEL_NAME)).strip() or _DEFAULT_KERNEL_NAME
        self._default_exec_timeout_sec = self._coerce_float(plugin_cfg.get("execution_timeout_sec"), _DEFAULT_EXEC_TIMEOUT_SEC, minimum=5.0)
        self._push_priority = self._coerce_int(plugin_cfg.get("push_priority"), 6, minimum=0, maximum=10)
        self._enable_content_push = self._coerce_bool(plugin_cfg.get("push_content_updates"), False)
        self._enable_model_push = self._coerce_bool(plugin_cfg.get("push_to_chat_model"), True)
        self._max_event_queue = self._coerce_int(plugin_cfg.get("max_event_queue"), _MAX_EVENT_QUEUE, minimum=100, maximum=5000)
        self._prewarm_kernel = self._coerce_bool(plugin_cfg.get("prewarm_kernel"), True)
        self._shared_kernel = self._coerce_bool(plugin_cfg.get("shared_kernel"), True)
        self._runtime_bootstrap_timeout_sec = self._coerce_int(
            plugin_cfg.get("bootstrap_timeout_sec"),
            _DEFAULT_BOOTSTRAP_TIMEOUT_SEC,
            minimum=60,
            maximum=1800,
        )

        env_state = await asyncio.to_thread(self._bootstrap_runtime_environment, plugin_cfg)
        self._runtime_ready = bool(env_state.get("ready", False))
        self._runtime_error = str(env_state.get("error", "") or "")

        if not any(self._workspace_root.rglob("*.ipynb")):
            default_path = self._workspace_root / _DEFAULT_NOTEBOOK_NAME
            default_notebook = self._build_default_notebook(title="N.E.K.O Notebook")
            self._write_notebook(default_path, default_notebook)

        ui_registered = self.register_static_ui("static", cache_control="no-store, no-cache, must-revalidate, max-age=0")
        self._start_event_worker()
        if self._prewarm_kernel and AsyncKernelManager is not None and self._runtime_ready:
            asyncio.create_task(self._prewarm_default_kernel())

        return Ok(
            {
                "status": "running",
                "ui_registered": ui_registered,
                "ui_url": self._plugin_ui_url(),
                "workspace": str(self._workspace_root),
                "kernel_available": AsyncKernelManager is not None and self._runtime_ready,
                "nbformat_available": nbformat is not None,
                "kernel_name": self._default_kernel_name,
                "execution_timeout_sec": self._default_exec_timeout_sec,
                "shared_kernel": self._shared_kernel,
                "runtime_env": env_state,
            }
        )

    @lifecycle(id="shutdown")
    async def shutdown(self, **_):
        self._event_stop.set()
        self._event_wake.set()
        if self._event_thread is not None and self._event_thread.is_alive():
            self._event_thread.join(timeout=3.0)
        await self._shutdown_all_kernels()
        return Ok({"status": "stopped"})

    async def _prewarm_default_kernel(self) -> None:
        try:
            notebooks = self._list_notebook_files()
            notebook_key = str(notebooks[0].get("path", _DEFAULT_NOTEBOOK_NAME)) if notebooks else _DEFAULT_NOTEBOOK_NAME
            await self._ensure_kernel(notebook_key)
            await self._warm_kernel_runtime(notebook_key)
        except Exception as exc:
            try:
                self.logger.info("Kernel prewarm skipped: {}", exc)
            except Exception:
                pass

    async def _warm_kernel_runtime(self, notebook_key: str) -> None:
        if self._kernel_hot_loaded:
            return

        warmup_code = (
            "import math\n"
            "import numpy as np\n"
            "import pandas as pd\n"
            "import matplotlib\n"
            "import matplotlib.pyplot as plt\n"
            "import seaborn as sns\n"
            "import plotly\n"
            "import plotly.graph_objects as go\n"
            "_neko_kernel_hot_ready = True\n"
        )

        try:
            result = await self._execute_code(self._kernel_key(notebook_key), warmup_code, timeout_sec=80.0)
            if bool(result.get("success", False)):
                self._kernel_hot_loaded = True
        except Exception:
            pass

    @plugin_entry(
        id="get_ui_info",
        name="获取 Notebook UI 信息",
        description="返回插件 UI 地址、运行能力与状态。",
        llm_result_fields=["ui_url", "kernel_available", "nbformat_available"],
    )
    async def get_ui_info(self, **_):
        return Ok(
            {
                "plugin_id": self.plugin_id,
                "ui_url": self._plugin_ui_url(),
                "ui_relative_url": f"/plugin/{self.plugin_id}/ui/",
                "workspace": str(self._workspace_root),
                "kernel_available": AsyncKernelManager is not None and self._runtime_ready,
                "nbformat_available": nbformat is not None,
                "kernel_name": self._default_kernel_name,
                "execution_timeout_sec": self._default_exec_timeout_sec,
                "shared_kernel": self._shared_kernel,
                "runtime_env": {
                    "ready": self._runtime_ready,
                    "venv_dir": str(self._venv_dir),
                    "venv_python": str(self._venv_python) if self._venv_python is not None else "",
                    "venv_python_exec": str(self._venv_python_exec or ""),
                    "error": self._runtime_error,
                },
            }
        )

    def _bootstrap_runtime_environment(self, plugin_cfg: dict[str, Any]) -> dict[str, Any]:
        auto_setup_env = self._coerce_bool(plugin_cfg.get("auto_setup_env"), True)
        auto_install_deps = self._coerce_bool(plugin_cfg.get("auto_install_deps"), True)

        self._runtime_root.mkdir(parents=True, exist_ok=True)
        self._venv_dir.mkdir(parents=True, exist_ok=True)

        created = False
        installed = False
        error = ""

        try:
            venv_python = self._resolve_venv_python(self._venv_dir)
            if venv_python is None and auto_setup_env:
                ok, _stdout, stderr = self._run_subprocess(
                    [sys.executable, "-m", "venv", str(self._venv_dir)],
                    timeout_sec=float(self._runtime_bootstrap_timeout_sec),
                )
                if not ok:
                    raise RuntimeError(f"create venv failed: {stderr}")
                created = True
                venv_python = self._resolve_venv_python(self._venv_dir)

            if venv_python is None:
                raise RuntimeError("venv python not found")

            self._venv_python = venv_python
            self._venv_python_exec = self._resolve_executable_for_subprocess(venv_python)
            self._inject_venv_site_packages(self._venv_dir, venv_python)

            deps_ready_subprocess, probe_detail = self._probe_venv_dependencies(venv_python)
            deps_ready = deps_ready_subprocess
            if (not deps_ready) and auto_install_deps:
                install_cmd = [
                    self._venv_python_exec or str(venv_python),
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    "--upgrade",
                    *_RUNTIME_REQUIREMENTS,
                ]
                ok, _stdout, stderr = self._run_subprocess(
                    install_cmd,
                    timeout_sec=float(self._runtime_bootstrap_timeout_sec),
                )
                if not ok:
                    raise RuntimeError(f"pip install failed: {stderr}")
                installed = True
                self._inject_venv_site_packages(self._venv_dir, venv_python)
                deps_ready_subprocess, probe_detail = self._probe_venv_dependencies(venv_python)
                deps_ready = deps_ready_subprocess

            deps_ready_in_process, in_process_detail = self._probe_imports_in_process(force_reload=True)
            deps_ready = bool(deps_ready and deps_ready_in_process)

            self._runtime_ready = bool(deps_ready)
            if not self._runtime_ready:
                error = (
                    "runtime dependencies unavailable in plugin process "
                    f"(probe={probe_detail}; in_process={in_process_detail})"
                )

        except Exception as exc:
            self._runtime_ready = False
            error = str(exc)

        self._runtime_error = error
        return {
            "ready": self._runtime_ready,
            "created_venv": created,
            "installed_deps": installed,
            "venv_dir": str(self._venv_dir),
            "venv_python": str(self._venv_python) if self._venv_python is not None else "",
            "venv_python_exec": str(self._venv_python_exec or ""),
            "auto_setup_env": auto_setup_env,
            "auto_install_deps": auto_install_deps,
            "error": error,
        }

    def _resolve_venv_python(self, venv_dir: Path) -> Path | None:
        if os.name == "nt":
            candidate = venv_dir / "Scripts" / "python.exe"
            return candidate if candidate.exists() else None
        candidate = venv_dir / "bin" / "python"
        return candidate if candidate.exists() else None

    def _resolve_executable_for_subprocess(self, python_path: Path) -> str:
        value = str(python_path)
        if os.name != "nt":
            return value

        # Use short path on Windows to reduce Unicode/path encoding issues in spawned subprocesses.
        try:
            import ctypes

            get_short = ctypes.windll.kernel32.GetShortPathNameW
            get_short.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint]
            get_short.restype = ctypes.c_uint

            needed = get_short(value, None, 0)
            if needed:
                buf = ctypes.create_unicode_buffer(needed)
                written = get_short(value, buf, needed)
                if written:
                    short_value = str(buf.value or "").strip()
                    if short_value:
                        return short_value
        except Exception:
            pass

        return value

    def _inject_venv_site_packages(self, venv_dir: Path, venv_python: Path | None = None) -> None:
        candidates: list[Path] = []
        if os.name == "nt":
            candidates.append(venv_dir / "Lib" / "site-packages")
        else:
            lib_dir = venv_dir / "lib"
            if lib_dir.exists():
                for child in lib_dir.iterdir():
                    if child.is_dir() and child.name.startswith("python"):
                        candidates.append(child / "site-packages")

        if venv_python is not None and venv_python.exists():
            query_cmd = [
                str(venv_python),
                "-c",
                (
                    "import json,site,sysconfig;"
                    "paths=[];"
                    "paths.extend(site.getsitepackages() if hasattr(site,'getsitepackages') else []);"
                    "cfg=sysconfig.get_paths();"
                    "paths.extend([cfg.get('purelib',''), cfg.get('platlib','')]);"
                    "print(json.dumps([p for p in dict.fromkeys(paths) if p]))"
                ),
            ]
            ok, stdout, _stderr = self._run_subprocess(query_cmd, timeout_sec=20.0)
            if ok and stdout.strip():
                try:
                    parsed = json.loads(stdout)
                    if isinstance(parsed, list):
                        for item in parsed:
                            if isinstance(item, str) and item.strip():
                                candidates.append(Path(item.strip()))
                except Exception:
                    pass

        for site_path in candidates:
            if not site_path.exists():
                continue
            value = str(site_path)
            if value not in sys.path:
                sys.path.insert(0, value)

    def _probe_venv_dependencies(self, venv_python: Path) -> tuple[bool, str]:
        cmd = [
            self._venv_python_exec or str(venv_python),
            "-c",
            (
                "import jupyter_client,ipykernel,nbformat,jupyter_server,jupyterlab,"
                "jupyterlab_server,notebook_shim,tornado,numpy,pandas,seaborn,plotly,"
                "ipywidgets,matplotlib,matplotlib_inline;print('ok')"
            ),
        ]
        ok, _stdout, stderr = self._run_subprocess(cmd, timeout_sec=45.0)
        detail = "probe-ok" if ok else (stderr.strip() or "probe-failed")
        return bool(ok), detail

    def _probe_imports_in_process(self, force_reload: bool = False) -> tuple[bool, str]:
        global AsyncKernelManager, nbformat

        try:
            if force_reload:
                conflict_roots = {
                    "attr",
                    "attrs",
                    "traitlets",
                    "jupyter_client",
                    "jupyter_core",
                    "ipykernel",
                    "nbformat",
                    "nest_asyncio",
                    "tornado",
                    "zmq",
                }
                for mod_name in list(sys.modules.keys()):
                    root = mod_name.split(".", 1)[0]
                    if root in conflict_roots:
                        sys.modules.pop(mod_name, None)
                AsyncKernelManager = None
                nbformat = None

                venv_site_markers: list[str] = []
                if os.name == "nt":
                    venv_site_markers.append(str((self._venv_dir / "Lib" / "site-packages").resolve()).lower())
                else:
                    lib_dir = self._venv_dir / "lib"
                    if lib_dir.exists():
                        for child in lib_dir.iterdir():
                            if child.is_dir() and child.name.startswith("python"):
                                venv_site_markers.append(str((child / "site-packages").resolve()).lower())

                filtered_paths: list[str] = []
                for value in sys.path:
                    path_text = str(value)
                    lower_path = path_text.lower()
                    if "site-packages" in lower_path and ("\\.venv\\" in lower_path or "/.venv/" in lower_path):
                        keep = any(lower_path == marker or lower_path.startswith(marker + os.sep) for marker in venv_site_markers)
                        if not keep:
                            continue
                    filtered_paths.append(path_text)
                sys.path[:] = filtered_paths

            self._inject_venv_site_packages(self._venv_dir, self._venv_python)
            importlib.invalidate_caches()

            if AsyncKernelManager is None:
                module = importlib.import_module("jupyter_client")
                manager_cls = getattr(module, "AsyncKernelManager", None)
                if manager_cls is None:
                    manager_mod = importlib.import_module("jupyter_client.manager")
                    manager_cls = getattr(manager_mod, "AsyncKernelManager", None)
                AsyncKernelManager = manager_cls
            if nbformat is None:
                nbformat = importlib.import_module("nbformat")
        except Exception as exc:
            return False, str(exc)

        if AsyncKernelManager is None:
            return False, "AsyncKernelManager is None"
        if nbformat is None:
            return False, "nbformat is None"
        return True, "ok"

    def _build_utf8_runtime_env(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        env = os.environ.copy()
        env.setdefault("PYTHONUTF8", "1")
        env.setdefault("PYTHONIOENCODING", "utf-8:replace")
        env.setdefault("PLOTLY_RENDERER", "plotly_mimetype")

        try:
            mpl_config_dir = self._runtime_root / "matplotlib"
            mpl_config_dir.mkdir(parents=True, exist_ok=True)
            env.setdefault("MPLCONFIGDIR", str(mpl_config_dir))
        except Exception:
            pass

        if isinstance(extra, dict):
            for key, value in extra.items():
                k = str(key or "").strip()
                if not k:
                    continue
                env[k] = str(value or "")
        return env

    def _run_subprocess(self, cmd: list[str], timeout_sec: float, env: dict[str, str] | None = None) -> tuple[bool, str, str]:
        try:
            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=max(1.0, float(timeout_sec)),
                check=False,
                env=self._build_utf8_runtime_env(env),
            )
            return proc.returncode == 0, str(proc.stdout or ""), str(proc.stderr or "")
        except Exception as exc:
            return False, "", str(exc)

    @plugin_entry(
        id="list_notebooks",
        name="列出 Notebook",
        description="列出插件工作目录中的所有 ipynb 文件。",
        llm_result_fields=["count"],
    )
    async def list_notebooks(self, **_):
        notebooks = self._list_notebook_files()
        return Ok({"count": len(notebooks), "notebooks": notebooks})

    @plugin_entry(
        id="create_notebook",
        name="创建 Notebook",
        description="创建一个新的 Notebook 文件。",
        llm_result_fields=["notebook_path", "ui_url"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string", "description": "相对路径，默认 Untitled.ipynb"},
                "title": {"type": "string", "description": "首个 markdown 标题"},
                "target_lanlan": {"type": "string", "description": "可选，主动推送目标角色名"},
            },
        },
    )
    async def create_notebook(self, notebook_path: str = "", title: str = "", target_lanlan: str = "", **kwargs):
        try:
            path = self._resolve_notebook_path(notebook_path)
            if path.exists():
                return Err(SdkError(f"Notebook already exists: {self._to_relative_notebook_path(path)}"))

            notebook = self._build_default_notebook(title=title.strip() or "N.E.K.O Notebook")
            self._write_notebook(path, notebook)
            rel = self._to_relative_notebook_path(path)

            self._enqueue_event(
                event_type="notebook_created",
                summary=f"Notebook created: {rel}",
                metadata={"notebook_path": rel, "cell_count": len(notebook.get("cells", []))},
                target_lanlan=self._resolve_target_lanlan(target_lanlan, kwargs),
            )

            return Ok(
                {
                    "notebook_path": rel,
                    "notebook": self._serialize_notebook_for_ui(notebook, rel),
                    "ui_url": self._plugin_ui_url(),
                }
            )
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to create notebook: {exc}"))

    @plugin_entry(
        id="delete_notebook",
        name="删除 Notebook",
        description="删除指定 Notebook 文件（不可恢复）。",
        llm_result_fields=["deleted_path", "count"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string", "description": "相对路径"},
                "target_lanlan": {"type": "string", "description": "可选，主动推送目标角色名"},
            },
            "required": ["notebook_path"],
        },
    )
    async def delete_notebook(self, notebook_path: str, target_lanlan: str = "", **kwargs):
        try:
            path = self._resolve_notebook_path(notebook_path)
            rel = self._to_relative_notebook_path(path)

            if (not path.exists()) or (not path.is_file()):
                return Err(SdkError(f"Notebook not found: {rel}"))

            await asyncio.to_thread(path.unlink)

            self._enqueue_event(
                event_type="notebook_deleted",
                summary=f"Notebook deleted: {rel}",
                metadata={"notebook_path": rel},
                target_lanlan=self._resolve_target_lanlan(target_lanlan, kwargs),
            )

            notebooks = self._list_notebook_files()
            return Ok(
                {
                    "deleted_path": rel,
                    "count": len(notebooks),
                    "notebooks": notebooks,
                }
            )
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to delete notebook: {exc}"))

    @plugin_entry(
        id="rename_notebook",
        name="重命名 Notebook",
        description="重命名指定 Notebook 文件。",
        llm_result_fields=["old_path", "renamed_path", "count"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string", "description": "当前相对路径"},
                "new_notebook_path": {"type": "string", "description": "新相对路径"},
                "target_lanlan": {"type": "string", "description": "可选，主动推送目标角色名"},
            },
            "required": ["notebook_path", "new_notebook_path"],
        },
    )
    async def rename_notebook(
        self,
        notebook_path: str,
        new_notebook_path: str,
        target_lanlan: str = "",
        **kwargs,
    ):
        try:
            old_path = self._resolve_notebook_path(notebook_path)
            new_path = self._resolve_notebook_path(new_notebook_path)
            old_rel = self._to_relative_notebook_path(old_path)
            new_rel = self._to_relative_notebook_path(new_path)

            if (not old_path.exists()) or (not old_path.is_file()):
                return Err(SdkError(f"Notebook not found: {old_rel}"))

            if old_path.resolve() == new_path.resolve():
                notebooks = self._list_notebook_files()
                return Ok(
                    {
                        "old_path": old_rel,
                        "renamed_path": new_rel,
                        "count": len(notebooks),
                        "notebooks": notebooks,
                    }
                )

            if new_path.exists():
                return Err(SdkError(f"Notebook already exists: {new_rel}"))

            new_path.parent.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(old_path.rename, new_path)

            self._enqueue_event(
                event_type="notebook_renamed",
                summary=f"Notebook renamed: {old_rel} -> {new_rel}",
                metadata={
                    "old_notebook_path": old_rel,
                    "new_notebook_path": new_rel,
                },
                target_lanlan=self._resolve_target_lanlan(target_lanlan, kwargs),
            )

            notebooks = self._list_notebook_files()
            return Ok(
                {
                    "old_path": old_rel,
                    "renamed_path": new_rel,
                    "count": len(notebooks),
                    "notebooks": notebooks,
                }
            )
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to rename notebook: {exc}"))

    @plugin_entry(
        id="read_notebook_raw_text",
        name="读取 Notebook 原始文本",
        description="返回 Notebook 原始 JSON 文本（不解析），用于前端 Worker 增量流式解析。",
        llm_result_fields=["notebook_path", "char_length", "file_size_bytes"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string", "description": "相对路径"},
            },
            "required": ["notebook_path"],
        },
    )
    async def read_notebook_raw_text(self, notebook_path: str, **_):
        try:
            path = self._resolve_notebook_path(notebook_path)
            rel = self._to_relative_notebook_path(path)
            text = await asyncio.to_thread(self._read_notebook_text, path)
            file_size = 0
            try:
                file_size = int(path.stat().st_size)
            except Exception:
                file_size = max(0, len(text.encode("utf-8", errors="ignore")))

            return Ok(
                {
                    "notebook_path": rel,
                    "text": text,
                    "char_length": len(text),
                    "file_size_bytes": file_size,
                }
            )
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to read notebook raw text: {exc}"))

    @plugin_entry(
        id="load_notebook_outline",
        name="加载 Notebook 轮廓",
        description="读取 Notebook 元数据与单元格轮廓（不含内容与输出），适用于超大文件快速打开。",
        llm_result_fields=["notebook_path", "cell_count"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string", "description": "相对路径"},
            },
            "required": ["notebook_path"],
        },
    )
    async def load_notebook_outline(self, notebook_path: str, **_):
        try:
            path = self._resolve_notebook_path(notebook_path)
            rel = self._to_relative_notebook_path(path)
            payload = await asyncio.to_thread(self._serialize_notebook_outline_streaming_for_ui, path, rel)
            return Ok(
                {
                    "notebook_path": rel,
                    "cell_count": len(payload.get("cells", [])),
                    "total_cell_count": int(payload.get("total_cell_count", 0) or 0),
                    "notebook": payload,
                }
            )
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to load notebook outline: {exc}"))

    @plugin_entry(
        id="load_notebook_cell_detail",
        name="按需加载单元格详情",
        description="按索引读取单元格详情，用于大文件按需展开。",
        llm_result_fields=["notebook_path", "cell_index"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string", "description": "相对路径"},
                "cell_index": {"type": "integer", "description": "单元格索引"},
            },
            "required": ["notebook_path", "cell_index"],
        },
    )
    async def load_notebook_cell_detail(self, notebook_path: str, cell_index: int, **_):
        try:
            path = self._resolve_notebook_path(notebook_path)
            rel = self._to_relative_notebook_path(path)
            payload = await asyncio.to_thread(self._serialize_single_cell_detail_streaming_for_ui, path, rel, cell_index)
            return Ok(
                {
                    "notebook_path": rel,
                    "cell_index": int(payload.get("cell_index", 0) or 0),
                    "total_cell_count": int(payload.get("total_cell_count", 0) or 0),
                    "notebook": {
                        "path": rel,
                        "cells": [payload.get("cell", {})],
                        "total_cell_count": int(payload.get("total_cell_count", 0) or 0),
                        "loaded_cell_offset": int(payload.get("cell_index", 0) or 0),
                        "loaded_cell_count": 1,
                        "metadata": payload.get("metadata", {}),
                        "nbformat": int(payload.get("nbformat", 4) or 4),
                        "nbformat_minor": int(payload.get("nbformat_minor", 5) or 5),
                    },
                }
            )
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to load notebook cell detail: {exc}"))

    @plugin_entry(
        id="load_notebook_media_payload",
        name="按需加载媒体载荷",
        description="按单元格索引读取大媒体内容载荷（如视频 base64）。",
        llm_result_fields=["notebook_path", "cell_index", "media_mime"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string", "description": "相对路径"},
                "cell_index": {"type": "integer", "description": "单元格索引"},
                "media_mime": {"type": "string", "description": "可选，目标媒体 MIME"},
            },
            "required": ["notebook_path", "cell_index"],
        },
    )
    async def load_notebook_media_payload(self, notebook_path: str, cell_index: int, media_mime: str = "", **_):
        try:
            path = self._resolve_notebook_path(notebook_path)
            rel = self._to_relative_notebook_path(path)
            payload = await asyncio.to_thread(self._load_cell_media_payload_streaming, path, rel, cell_index, media_mime)
            return Ok(payload)
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to load notebook media payload: {exc}"))

    @plugin_entry(
        id="store_notebook_media_asset",
        name="保存 Notebook 媒体资产",
        description="将媒体数据保存到工作区根目录 assets/image|video，并返回相对路径与渲染 HTML。",
        llm_result_fields=["notebook_path", "asset_rel_path", "asset_kind", "size_bytes"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string", "description": "相对路径"},
                "media_mime": {"type": "string", "description": "媒体 MIME"},
                "base64": {"type": "string", "description": "媒体 base64 数据"},
                "original_name": {"type": "string", "description": "原始文件名（可选）"},
            },
            "required": ["notebook_path", "media_mime", "base64"],
        },
    )
    async def store_notebook_media_asset(
        self,
        notebook_path: str,
        media_mime: str,
        base64: str,
        original_name: str = "",
        **_,
    ):
        try:
            path = self._resolve_notebook_path(notebook_path)
            rel = self._to_relative_notebook_path(path)
            payload = await asyncio.to_thread(
                self._store_media_asset_for_notebook,
                path,
                rel,
                media_mime,
                base64,
                original_name,
            )
            return Ok(payload)
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to store notebook media asset: {exc}"))

    @plugin_entry(
        id="store_notebook_media_asset_from_path",
        name="从文件路径保存 Notebook 媒体资产",
        description="将本地媒体文件复制到工作区根目录 assets/image|video，并返回相对路径与渲染 HTML。",
        llm_result_fields=["notebook_path", "asset_rel_path", "asset_kind", "size_bytes"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string", "description": "相对路径"},
                "source_path": {"type": "string", "description": "本地源文件路径"},
                "media_mime": {"type": "string", "description": "可选，媒体 MIME"},
                "original_name": {"type": "string", "description": "原始文件名（可选）"},
            },
            "required": ["notebook_path", "source_path"],
        },
    )
    async def store_notebook_media_asset_from_path(
        self,
        notebook_path: str,
        source_path: str,
        media_mime: str = "",
        original_name: str = "",
        **_,
    ):
        try:
            path = self._resolve_notebook_path(notebook_path)
            rel = self._to_relative_notebook_path(path)
            payload = await asyncio.to_thread(
                self._store_media_asset_for_notebook_from_path,
                path,
                rel,
                source_path,
                media_mime,
                original_name,
            )
            return Ok(payload)
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to store notebook media asset from path: {exc}"))

    @plugin_entry(
        id="load_notebook_media_asset_payload",
        name="读取 Notebook 媒体资产",
        description="按相对路径读取已落盘媒体并返回 base64 载荷。",
        llm_result_fields=["notebook_path", "asset_rel_path", "media_mime", "size_bytes"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string", "description": "相对路径"},
                "asset_rel_path": {"type": "string", "description": "相对媒体路径，如 ./assets/video/x.mp4"},
            },
            "required": ["notebook_path", "asset_rel_path"],
        },
    )
    async def load_notebook_media_asset_payload(self, notebook_path: str, asset_rel_path: str, **_):
        try:
            path = self._resolve_notebook_path(notebook_path)
            rel = self._to_relative_notebook_path(path)
            payload = await asyncio.to_thread(self._read_media_asset_payload_for_notebook, path, rel, asset_rel_path)
            return Ok(payload)
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to load notebook media asset payload: {exc}"))

    @plugin_entry(
        id="load_notebook",
        name="加载 Notebook",
        description="读取 Notebook 文件并返回结构化单元格数据。",
        llm_result_fields=["notebook_path", "cell_count"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string", "description": "相对路径"},
                "cell_offset": {"type": "integer", "description": "可选：按偏移分片读取"},
                "cell_limit": {"type": "integer", "description": "可选：单次最多返回单元格数量"},
            },
            "required": ["notebook_path"],
        },
    )
    async def load_notebook(self, notebook_path: str, cell_offset: int = 0, cell_limit: int = 0, **_):
        try:
            path = self._resolve_notebook_path(notebook_path)
            notebook = await asyncio.to_thread(self._read_notebook, path)
            rel = self._to_relative_notebook_path(path)
            total_cells = len(self._cells_from_notebook(notebook))

            offset_value = self._coerce_int(cell_offset, 0, minimum=0, maximum=max(0, total_cells))
            limit_value = self._coerce_int(cell_limit, 0, minimum=0, maximum=2000)
            limit_arg: int | None = limit_value if limit_value > 0 else None

            payload = await asyncio.to_thread(
                self._serialize_notebook_for_ui,
                notebook,
                rel,
                offset_value,
                limit_arg,
            )
            return Ok(
                {
                    "notebook_path": rel,
                    "cell_count": len(payload.get("cells", [])),
                    "total_cell_count": total_cells,
                    "cell_offset": offset_value,
                    "cell_limit": limit_value,
                    "notebook": payload,
                }
            )
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to load notebook: {exc}"))

    @plugin_entry(
        id="save_notebook",
        name="保存 Notebook",
        description="保存 Notebook 的完整单元格内容。",
        llm_result_fields=["notebook_path", "cell_count"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string"},
                "cells": {"type": "array"},
                "metadata": {"type": "object"},
                "target_lanlan": {"type": "string"},
            },
            "required": ["notebook_path", "cells"],
        },
    )
    async def save_notebook(
        self,
        notebook_path: str,
        cells: list[dict[str, Any]],
        metadata: dict[str, Any] | None = None,
        target_lanlan: str = "",
        **kwargs,
    ):
        try:
            path = self._resolve_notebook_path(notebook_path)
            notebook = self._read_notebook(path)
            previous_asset_paths = self._collect_notebook_asset_paths(notebook, path)
            notebook["cells"] = self._normalize_cells_payload(cells)

            meta_obj = notebook.get("metadata")
            meta: dict[str, Any] = dict(meta_obj) if isinstance(meta_obj, dict) else {}
            if isinstance(metadata, dict):
                for key, value in metadata.items():
                    if key in {"kernelspec", "language_info", "title"}:
                        meta[key] = value
            notebook["metadata"] = self._merge_notebook_metadata(meta)

            self._write_notebook(path, notebook)
            current_asset_paths = self._collect_notebook_asset_paths(notebook, path)
            stale_asset_paths = previous_asset_paths - current_asset_paths
            if stale_asset_paths:
                self._delete_asset_files(stale_asset_paths)
            rel = self._to_relative_notebook_path(path)

            self._enqueue_event(
                event_type="notebook_saved",
                summary=f"Notebook saved: {rel}",
                metadata={
                    "notebook_path": rel,
                    "cell_count": len(notebook.get("cells", [])),
                    "title": str((notebook.get("metadata") or {}).get("title", "")),
                },
                target_lanlan=self._resolve_target_lanlan(target_lanlan, kwargs),
            )

            return Ok(
                {
                    "notebook_path": rel,
                    "cell_count": len(notebook.get("cells", [])),
                    "notebook": self._serialize_notebook_for_ui(notebook, rel),
                }
            )
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to save notebook: {exc}"))

    @plugin_entry(
        id="add_cell",
        name="新增单元格",
        description="在指定位置新增代码或 markdown 单元格。",
        llm_result_fields=["notebook_path", "cell_id", "cell_type"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string"},
                "cell_type": {"type": "string", "enum": ["code", "markdown", "view"], "default": "code"},
                "source": {"type": "string", "default": ""},
                "index": {"type": "integer", "default": -1},
                "target_lanlan": {"type": "string"},
            },
            "required": ["notebook_path"],
        },
    )
    async def add_cell(
        self,
        notebook_path: str,
        cell_type: str = "code",
        source: str = "",
        index: int = -1,
        target_lanlan: str = "",
        **kwargs,
    ):
        try:
            path = self._resolve_notebook_path(notebook_path)
            notebook = self._read_notebook(path)
            cells = self._cells_from_notebook(notebook)

            normalized_type = self._normalize_cell_type(cell_type)
            cell = self._new_cell(normalized_type, source)

            insert_index = int(index)
            if insert_index < 0 or insert_index > len(cells):
                cells.append(cell)
            else:
                cells.insert(insert_index, cell)

            notebook["cells"] = cells
            self._write_notebook(path, notebook)
            rel = self._to_relative_notebook_path(path)

            self._enqueue_event(
                event_type="cell_added",
                summary=f"Cell added in {rel}",
                metadata={
                    "notebook_path": rel,
                    "cell_id": cell["metadata"]["id"],
                    "cell_type": normalized_type,
                    "source_preview": self._clip_text(self._source_to_text(cell.get("source")), 180),
                },
                target_lanlan=self._resolve_target_lanlan(target_lanlan, kwargs),
            )

            return Ok(
                {
                    "notebook_path": rel,
                    "cell_id": cell["metadata"]["id"],
                    "cell_type": normalized_type,
                    "notebook": self._serialize_notebook_for_ui(notebook, rel),
                }
            )
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to add cell: {exc}"))

    @plugin_entry(
        id="delete_cell",
        name="删除单元格",
        description="按 cell_id 删除单元格。",
        llm_result_fields=["notebook_path", "deleted"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string"},
                "cell_id": {"type": "string"},
                "cell_index": {"type": "integer", "description": "可选：当 cell_id 失配时按索引删除"},
                "include_notebook": {"type": "boolean", "default": True},
                "target_lanlan": {"type": "string"},
            },
            "required": ["notebook_path", "cell_id"],
        },
    )
    async def delete_cell(
        self,
        notebook_path: str,
        cell_id: str,
        cell_index: int | None = None,
        include_notebook: bool = True,
        target_lanlan: str = "",
        **kwargs,
    ):
        try:
            path = self._resolve_notebook_path(notebook_path)
            notebook = self._read_notebook(path)
            cells = self._cells_from_notebook(notebook)
            before = len(cells)
            deleted_cell_obj: dict[str, Any] | None = None
            filtered = [c for c in cells if str((c.get("metadata") or {}).get("id", "")) != str(cell_id)]
            deleted_id = str(cell_id)
            if len(filtered) == before:
                idx: int | None = None
                try:
                    if cell_index is not None:
                        idx = int(cell_index)
                except (TypeError, ValueError):
                    idx = None

                if idx is None or idx < 0 or idx >= before:
                    return Err(SdkError(f"Cell not found: {cell_id}"))

                target = cells[idx] if 0 <= idx < len(cells) else None
                if not isinstance(target, dict):
                    return Err(SdkError(f"Cell not found: {cell_id}"))

                deleted_id = str(((target.get("metadata") or {}).get("id", "") or cell_id))
                deleted_cell_obj = target
                filtered = [c for i, c in enumerate(cells) if i != idx]
            else:
                for c in cells:
                    if str((c.get("metadata") or {}).get("id", "")) == str(cell_id):
                        deleted_cell_obj = c
                        break

            notebook["cells"] = filtered
            self._write_notebook(path, notebook)

            if isinstance(deleted_cell_obj, dict):
                removed_assets = self._collect_cell_asset_paths(deleted_cell_obj, path)
                if removed_assets:
                    remaining_assets = self._collect_notebook_asset_paths(notebook, path)
                    stale_assets = removed_assets - remaining_assets
                    if stale_assets:
                        self._delete_asset_files(stale_assets)

            rel = self._to_relative_notebook_path(path)

            self._enqueue_event(
                event_type="cell_deleted",
                summary=f"Cell deleted in {rel}",
                metadata={"notebook_path": rel, "cell_id": deleted_id, "remaining": len(filtered)},
                target_lanlan=self._resolve_target_lanlan(target_lanlan, kwargs),
            )

            payload: dict[str, Any] = {
                "notebook_path": rel,
                "deleted": deleted_id,
                "remaining": len(filtered),
            }
            if bool(include_notebook):
                payload["notebook"] = self._serialize_notebook_for_ui(notebook, rel)
            return Ok(payload)
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to delete cell: {exc}"))

    @plugin_entry(
        id="update_cell",
        name="更新单元格",
        description="更新单元格内容与类型。",
        llm_result_fields=["notebook_path", "cell_id"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string"},
                "cell_id": {"type": "string"},
                "cell_index": {"type": "integer", "description": "可选：当 cell_id 失配时按索引更新"},
                "source": {"type": "string"},
                "cell_type": {"type": "string", "enum": ["code", "markdown", "view"]},
                "emit_event": {"type": "boolean", "default": True},
                "include_notebook": {"type": "boolean", "default": True},
                "target_lanlan": {"type": "string"},
            },
            "required": ["notebook_path", "cell_id"],
        },
    )
    async def update_cell(
        self,
        notebook_path: str,
        cell_id: str,
        cell_index: int | None = None,
        source: str = "",
        cell_type: str = "",
        emit_event: bool = True,
        include_notebook: bool = True,
        target_lanlan: str = "",
        **kwargs,
    ):
        try:
            path = self._resolve_notebook_path(notebook_path)
            notebook = self._read_notebook(path)
            cells = self._cells_from_notebook(notebook)

            target_cell = None
            for cell in cells:
                cid = str((cell.get("metadata") or {}).get("id", ""))
                if cid == str(cell_id):
                    target_cell = cell
                    break

            if target_cell is None:
                idx: int | None = None
                try:
                    if cell_index is not None:
                        idx = int(cell_index)
                except (TypeError, ValueError):
                    idx = None

                if idx is None or idx < 0 or idx >= len(cells):
                    return Err(SdkError(f"Cell not found: {cell_id}"))

                maybe_cell = cells[idx]
                if not isinstance(maybe_cell, dict):
                    return Err(SdkError(f"Cell not found: {cell_id}"))
                target_cell = maybe_cell

                meta = self._dict_or_empty(target_cell.get("metadata"))
                resolved_id = str(meta.get("id", "")).strip()
                if not resolved_id:
                    resolved_id = uuid.uuid4().hex[:12]
                    meta["id"] = resolved_id
                    target_cell["metadata"] = meta
                cell_id = resolved_id

            if cell_type:
                normalized_type = self._normalize_cell_type(cell_type)
                target_cell["cell_type"] = normalized_type
                meta = self._dict_or_empty(target_cell.get("metadata"))
                meta["language"] = "python" if normalized_type == "code" else "markdown"
                target_cell["metadata"] = meta
                if normalized_type != "code":
                    target_cell.pop("outputs", None)
                    target_cell["execution_count"] = None
                else:
                    target_cell["outputs"] = target_cell.get("outputs") if isinstance(target_cell.get("outputs"), list) else []

            target_cell["source"] = self._source_to_lines(source)
            self._write_notebook(path, notebook)
            rel = self._to_relative_notebook_path(path)

            resolved_target = self._resolve_target_lanlan(target_lanlan, kwargs)
            if emit_event and self._enable_content_push:
                throttle_key = f"{rel}:{cell_id}"
                now_mono = time.monotonic()
                last = self._last_content_push_at.get(throttle_key, 0.0)
                if (now_mono - last) >= _CONTENT_PUSH_COOLDOWN_SEC:
                    self._last_content_push_at[throttle_key] = now_mono
                    current_type = self._normalize_cell_type(target_cell.get("cell_type", "markdown"))
                    if current_type in {"markdown", "view"}:
                        title = self._extract_markdown_title(source)
                        self._enqueue_event(
                            event_type="markdown_cell_updated",
                            summary=f"Markdown updated in {rel}",
                            metadata={
                                "notebook_path": rel,
                                "cell_id": cell_id,
                                "cell_type": current_type,
                                "markdown_title": title,
                                "markdown_content": source,
                                "content_length": len(source),
                            },
                            target_lanlan=resolved_target,
                        )
                    else:
                        self._enqueue_event(
                            event_type="code_cell_updated",
                            summary=f"Code cell updated in {rel}",
                            metadata={
                                "notebook_path": rel,
                                "cell_id": cell_id,
                                "cell_type": "code",
                                "code_content": source,
                                "content_length": len(source),
                                "content_preview": self._clip_text(source, 260),
                            },
                            target_lanlan=resolved_target,
                        )

            payload: dict[str, Any] = {
                "notebook_path": rel,
                "cell_id": cell_id,
            }
            if bool(include_notebook):
                payload["notebook"] = self._serialize_notebook_for_ui(notebook, rel)
            return Ok(payload)
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to update cell: {exc}"))

    @plugin_entry(
        id="execute_cell",
        name="执行单元格",
        description="使用 Jupyter 内核执行代码单元格并回填输出。",
        llm_result_fields=["notebook_path", "cell_id", "success", "output_text"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string"},
                "cell_id": {"type": "string"},
                "cell_index": {"type": "integer", "description": "可选：当 cell_id 失配时按索引执行"},
                "source": {"type": "string", "description": "可选：本次执行前覆盖单元格源码"},
                "timeout_sec": {"type": "number", "default": 90},
                "include_notebook": {"type": "boolean", "default": True},
                "target_lanlan": {"type": "string"},
            },
            "required": ["notebook_path", "cell_id"],
        },
    )
    async def execute_cell(
        self,
        notebook_path: str,
        cell_id: str,
        cell_index: int | None = None,
        source: str = "",
        timeout_sec: float | int | str | None = None,
        include_notebook: bool = True,
        target_lanlan: str = "",
        **kwargs,
    ):
        if self._venv_python is None:
            reason = self._runtime_error or "isolated runtime python is not available"
            return Err(SdkError(reason))

        resolved_target = self._resolve_target_lanlan(target_lanlan, kwargs)
        timeout_value = self._coerce_float(timeout_sec, self._default_exec_timeout_sec, minimum=5.0)

        try:
            perf_started = time.monotonic()
            perf_stage_ms: dict[str, int] = {}

            read_started = time.monotonic()
            path = self._resolve_notebook_path(notebook_path)
            notebook = self._read_notebook(path)
            cells = self._cells_from_notebook(notebook)
            perf_stage_ms["read_notebook_ms"] = int((time.monotonic() - read_started) * 1000)

            resolve_started = time.monotonic()
            target_cell = None
            for cell in cells:
                if str((cell.get("metadata") or {}).get("id", "")) == str(cell_id):
                    target_cell = cell
                    break

            if target_cell is None:
                idx: int | None = None
                try:
                    if cell_index is not None:
                        idx = int(cell_index)
                except (TypeError, ValueError):
                    idx = None

                if idx is None or idx < 0 or idx >= len(cells):
                    return Err(SdkError(f"Cell not found: {cell_id}"))

                maybe_cell = cells[idx]
                if not isinstance(maybe_cell, dict):
                    return Err(SdkError(f"Cell not found: {cell_id}"))
                target_cell = maybe_cell

                meta = self._dict_or_empty(target_cell.get("metadata"))
                resolved_id = str(meta.get("id", "")).strip()
                if not resolved_id:
                    resolved_id = uuid.uuid4().hex[:12]
                    meta["id"] = resolved_id
                    target_cell["metadata"] = meta
                cell_id = resolved_id

            perf_stage_ms["resolve_cell_ms"] = int((time.monotonic() - resolve_started) * 1000)

            if str(target_cell.get("cell_type", "")) != "code":
                return Err(SdkError("Only code cell can be executed"))

            rel = self._to_relative_notebook_path(path)
            source_text = self._source_to_text(target_cell.get("source"))
            if str(source or "") != "":
                source_text = self._source_to_text(source)
                target_cell["source"] = self._source_to_lines(source_text)

            self._enqueue_event(
                event_type="cell_execution_status",
                summary=f"Cell queued: {cell_id}",
                metadata={"notebook_path": rel, "cell_id": cell_id, "cell_type": "code", "status": "queued"},
                target_lanlan=resolved_target,
            )
            self._enqueue_event(
                event_type="cell_execution_status",
                summary=f"Cell running: {cell_id}",
                metadata={"notebook_path": rel, "cell_id": cell_id, "cell_type": "code", "status": "running"},
                target_lanlan=resolved_target,
            )

            async def _on_stream(stream_name: str, chunk_text: str, elapsed_ms: int) -> None:
                self._enqueue_event(
                    event_type="cell_execution_stream",
                    summary=f"Cell stream: {cell_id}",
                    metadata={
                        "notebook_path": rel,
                        "cell_id": cell_id,
                        "cell_type": "code",
                        "stream": stream_name,
                        "elapsed_ms": elapsed_ms,
                        "chunk": chunk_text,
                    },
                    target_lanlan=resolved_target,
                )

            use_kernel_backend = AsyncKernelManager is not None and self._runtime_ready
            kernel_key = self._kernel_key(rel)
            execute_backend_started = time.monotonic()
            if use_kernel_backend:
                if self._shared_kernel and (not self._kernel_hot_loaded):
                    try:
                        await self._warm_kernel_runtime(kernel_key)
                    except Exception:
                        pass
                try:
                    result = await self._execute_code(kernel_key, source_text, timeout_value, on_stream=_on_stream)
                    result["backend"] = "kernel"
                except Exception as exc:
                    retry_reason = str(exc)
                    try:
                        await self._restart_kernel_for_notebook(kernel_key)
                        result = await self._execute_code(kernel_key, source_text, timeout_value, on_stream=_on_stream)
                        result["backend"] = "kernel"
                    except Exception as retry_exc:
                        result = await asyncio.to_thread(
                            self._execute_code_subprocess_fallback,
                            source_text,
                            timeout_value,
                            f"{retry_reason}; kernel_retry_failed: {retry_exc}",
                        )
            else:
                result = await asyncio.to_thread(
                    self._execute_code_subprocess_fallback,
                    source_text,
                    timeout_value,
                    self._runtime_error or "kernel backend unavailable",
                )
            perf_stage_ms["backend_execute_ms"] = int((time.monotonic() - execute_backend_started) * 1000)

            outputs_raw = result.get("outputs")
            outputs: list[dict[str, Any]] = []
            if isinstance(outputs_raw, list):
                for item in outputs_raw:
                    if isinstance(item, dict):
                        outputs.append(self._sanitize_json_value(dict(item)))
            exec_count = result.get("execution_count")
            success = bool(result.get("success", False))
            status_text = self._sanitize_text(result.get("status", "ok"))
            duration_ms = int(result.get("duration_ms") or 0)
            started_at = self._sanitize_text(result.get("started_at") or "")
            finished_at = self._sanitize_text(result.get("finished_at") or "")
            stdout_text = self._sanitize_text(result.get("stdout_text") or "")
            stderr_text = self._sanitize_text(result.get("stderr_text") or "")
            backend = self._sanitize_text(result.get("backend") or ("kernel" if use_kernel_backend else "subprocess"))

            target_cell["outputs"] = outputs
            target_cell["execution_count"] = exec_count

            write_started = time.monotonic()
            self._write_notebook(path, notebook)
            perf_stage_ms["write_notebook_ms"] = int((time.monotonic() - write_started) * 1000)
            output_text = self._outputs_to_text(outputs)
            has_visual_output = self._outputs_contain_visual(outputs)

            if success:
                self._enqueue_event(
                    event_type="cell_execution_status",
                    summary=f"Cell succeeded: {cell_id}",
                    metadata={"notebook_path": rel, "cell_id": cell_id, "cell_type": "code", "status": "succeeded"},
                    target_lanlan=resolved_target,
                )
                self._enqueue_event(
                    event_type="cell_execution_output",
                    summary=f"Execution output from {cell_id}",
                    metadata={
                        "notebook_path": rel,
                        "cell_id": cell_id,
                        "cell_type": "code",
                        "success": True,
                        "status": status_text,
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "duration_ms": duration_ms,
                        "execution_count": exec_count,
                        "execution_backend": backend,
                        "code_content": source_text,
                        "output_text": output_text,
                        "has_visual_output": has_visual_output,
                        "stdout": stdout_text,
                        "stderr": stderr_text,
                    },
                    target_lanlan=resolved_target,
                )
            else:
                self._enqueue_event(
                    event_type="cell_execution_status",
                    summary=f"Cell failed: {cell_id}",
                    metadata={"notebook_path": rel, "cell_id": cell_id, "cell_type": "code", "status": "failed"},
                    target_lanlan=resolved_target,
                )
                self._enqueue_event(
                    event_type="cell_execution_output",
                    summary=f"Execution error from {cell_id}",
                    metadata={
                        "notebook_path": rel,
                        "cell_id": cell_id,
                        "cell_type": "code",
                        "success": False,
                        "status": status_text,
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "duration_ms": duration_ms,
                        "execution_count": exec_count,
                        "execution_backend": backend,
                        "code_content": source_text,
                        "output_text": output_text,
                        "has_visual_output": has_visual_output,
                        "stdout": stdout_text,
                        "stderr": stderr_text,
                        "error": output_text or status_text,
                    },
                    target_lanlan=resolved_target,
                )

            cell_payload = {
                "id": cell_id,
                "cell_type": "code",
                "metadata": self._dict_or_empty(target_cell.get("metadata")),
                "source": self._source_to_text(target_cell.get("source")),
                "outputs": outputs,
                "output_text": output_text,
                "execution_count": exec_count,
            }

            kernel_stage_obj = result.get("kernel_stage_ms") if isinstance(result.get("kernel_stage_ms"), dict) else {}
            kernel_stage_ms: dict[str, int] = {}
            if isinstance(kernel_stage_obj, dict):
                for key, value in kernel_stage_obj.items():
                    try:
                        if value is None:
                            continue
                        kernel_stage_ms[str(key)] = int(value)
                    except (TypeError, ValueError):
                        continue

            first_parent_msg_ms = int(kernel_stage_ms.get("first_parent_msg_ms", 0)) if kernel_stage_ms else 0
            first_payload_msg_ms = int(kernel_stage_ms.get("first_payload_msg_ms", 0)) if kernel_stage_ms else 0
            execute_to_idle_ms = int(kernel_stage_ms.get("execute_to_idle_ms", 0)) if kernel_stage_ms else 0
            shell_status_ms = int(kernel_stage_ms.get("shell_status_ms", 0)) if kernel_stage_ms else 0
            pre_dispatch_ms = int(kernel_stage_ms.get("pre_dispatch_ms", 0)) if kernel_stage_ms else 0

            dispatch_to_kernel_receive_ms = max(0, pre_dispatch_ms + first_parent_msg_ms)
            kernel_receive_to_kernel_execute_ms = max(0, first_payload_msg_ms - first_parent_msg_ms) if first_payload_msg_ms > 0 else 0
            kernel_execute_to_result_return_ms = max(
                0,
                (execute_to_idle_ms - max(first_payload_msg_ms, first_parent_msg_ms))
                + shell_status_ms
                + int(perf_stage_ms.get("write_notebook_ms", 0)),
            )

            perf_stage_ms["server_total_ms"] = int((time.monotonic() - perf_started) * 1000)
            perf_trace = {
                "timeline_ms": {
                    "dispatch_to_kernel_receive_ms": dispatch_to_kernel_receive_ms,
                    "kernel_receive_to_kernel_execute_ms": kernel_receive_to_kernel_execute_ms,
                    "kernel_execute_to_result_return_ms": kernel_execute_to_result_return_ms,
                },
                "server_stage_ms": perf_stage_ms,
                "kernel_stage_ms": kernel_stage_ms,
            }

            payload: dict[str, Any] = {
                "notebook_path": rel,
                "cell_id": cell_id,
                "success": success,
                "status": status_text,
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_ms": duration_ms,
                "stdout": stdout_text,
                "stderr": stderr_text,
                "execution_backend": backend,
                "execution_count": exec_count,
                "output_text": output_text,
                "outputs": outputs,
                "cell": cell_payload,
                "perf_trace": self._sanitize_json_value(perf_trace),
            }
            if bool(include_notebook):
                payload["notebook"] = self._serialize_notebook_for_ui(notebook, rel)
            return Ok(payload)
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to execute cell: {exc}"))

    @plugin_entry(
        id="restart_kernel",
        name="重启内核",
        description="重启当前 Notebook 的执行内核。",
        llm_result_fields=["notebook_path", "kernel_status"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string"},
                "target_lanlan": {"type": "string"},
            },
            "required": ["notebook_path"],
        },
    )
    async def restart_kernel(self, notebook_path: str, target_lanlan: str = "", **kwargs):
        if AsyncKernelManager is None or not self._runtime_ready:
            reason = self._runtime_error or "jupyter runtime is not available in isolated env"
            return Err(SdkError(reason))

        try:
            path = self._resolve_notebook_path(notebook_path)
            rel = self._to_relative_notebook_path(path)
            await self._restart_kernel_for_notebook(rel)
            self._enqueue_event(
                event_type="kernel_restarted",
                summary=f"Kernel restarted for {rel}",
                metadata={"notebook_path": rel, "kernel": self._default_kernel_name},
                target_lanlan=self._resolve_target_lanlan(target_lanlan, kwargs),
            )
            return Ok({"notebook_path": rel, "kernel_status": "restarted", "kernel": self._default_kernel_name})
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to restart kernel: {exc}"))

    @plugin_entry(
        id="shutdown_kernel",
        name="关闭内核",
        description="关闭指定 Notebook 对应的执行内核。",
        llm_result_fields=["notebook_path", "kernel_status"],
        input_schema={
            "type": "object",
            "properties": {
                "notebook_path": {"type": "string"},
                "target_lanlan": {"type": "string"},
            },
            "required": ["notebook_path"],
        },
    )
    async def shutdown_kernel(self, notebook_path: str, target_lanlan: str = "", **kwargs):
        try:
            path = self._resolve_notebook_path(notebook_path)
            rel = self._to_relative_notebook_path(path)
            closed = await self._shutdown_kernel(rel)
            self._enqueue_event(
                event_type="kernel_shutdown",
                summary=f"Kernel shutdown for {rel}",
                metadata={"notebook_path": rel, "closed": bool(closed)},
                target_lanlan=self._resolve_target_lanlan(target_lanlan, kwargs),
            )
            return Ok({"notebook_path": rel, "kernel_status": "shutdown" if closed else "not-running"})
        except SdkError as exc:
            return Err(exc)
        except Exception as exc:
            return Err(SdkError(f"Failed to shutdown kernel: {exc}"))

    @plugin_entry(
        id="read_clipboard_media_files",
        name="读取剪贴板媒体文件",
        description="读取系统剪贴板中的图片/视频文件并返回可直接嵌入输出区的 base64 数据。",
        llm_result_fields=["count"],
        input_schema={
            "type": "object",
            "properties": {
                "max_files": {"type": "integer", "default": 8},
                "max_bytes": {"type": "integer", "default": _CLIPBOARD_MEDIA_MAX_BYTES},
            },
        },
    )
    async def read_clipboard_media_files(self, max_files: int = 8, max_bytes: int = _CLIPBOARD_MEDIA_MAX_BYTES, **_):
        file_paths = self._read_clipboard_file_paths()
        if not file_paths:
            return Ok({"count": 0, "media_items": [], "skipped": []})

        max_files_value = self._coerce_int(max_files, 8, minimum=1, maximum=24)
        max_bytes_value = self._coerce_int(max_bytes, _CLIPBOARD_MEDIA_MAX_BYTES, minimum=1024 * 64, maximum=512 * 1024 * 1024)

        media_items: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        for path_text in file_paths:
            if len(media_items) >= max_files_value:
                break

            path = Path(str(path_text or "")).expanduser()
            if (not path.exists()) or (not path.is_file()):
                skipped.append({"path": str(path), "reason": "not_found_or_not_file"})
                continue

            media_mime = self._guess_media_mime(path)
            if not media_mime:
                skipped.append({"path": str(path), "reason": "unsupported_media_type"})
                continue

            try:
                size = int(path.stat().st_size)
            except Exception:
                skipped.append({"path": str(path), "reason": "stat_failed"})
                continue

            if size <= 0:
                skipped.append({"path": str(path), "reason": "empty_file"})
                continue

            if size > max_bytes_value:
                skipped.append({"path": str(path), "reason": "file_too_large", "size": size})
                continue

            try:
                raw = await asyncio.to_thread(path.read_bytes)
                encoded = base64.b64encode(raw).decode("ascii")
            except Exception as exc:
                skipped.append({"path": str(path), "reason": f"read_failed: {exc}"})
                continue

            media_items.append(
                {
                    "name": path.name,
                    "path": str(path),
                    "mime": media_mime,
                    "size": size,
                    "base64": encoded,
                }
            )

        return Ok(
            {
                "count": len(media_items),
                "media_items": media_items,
                "scanned_paths": [str(x) for x in file_paths],
                "skipped": skipped,
            }
        )

    @plugin_entry(
        id="list_recent_events",
        name="查看最近事件",
        description="查看插件最近记录的主动推送事件。",
        llm_result_fields=["count"],
    )
    async def list_recent_events(self, limit: int = 50, **_):
        limit_value = self._coerce_int(limit, 50, minimum=1, maximum=_MAX_RECENT_EVENTS)
        with self._event_lock:
            items = list(self._recent_events)[-limit_value:]
        return Ok({"count": len(items), "events": items})

    @plugin_entry(
        id="send_ai_message",
        name="发送 AI 对话消息",
        description="将 Notebook 面板中的用户对话内容推送给 NEKO 主对话模型。",
        llm_result_fields=["accepted", "message_id"],
        input_schema={
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "用户输入的完整对话文本"},
                "user_identity": {"type": "string", "default": "用户", "description": "消息身份标识"},
                "message_id": {"type": "string", "description": "客户端生成的消息 ID（可选）"},
                "target_lanlan": {"type": "string", "description": "可选，指定目标角色会话"},
            },
            "required": ["text"],
        },
    )
    async def send_ai_message(
        self,
        text: str,
        user_identity: str = "用户",
        message_id: str = "",
        target_lanlan: str = "",
        **kwargs,
    ):
        text_value = self._sanitize_text(text).strip()
        if not text_value:
            return Err(SdkError("Message text is empty"))

        user_label = self._sanitize_text(user_identity).strip() or "用户"
        msg_id = str(message_id or "").strip() or uuid.uuid4().hex[:16]
        resolved_target = self._resolve_target_lanlan(target_lanlan, kwargs)

        content_lines = [
            "Jupyter Notebook 用户消息转发（原文）",
            "系统提示：这是一条用户原始输入，不是助手已完成行为，禁止改写为“我刚刚做了某操作”。",
            f"用户身份: {user_label}",
            "用户原文:",
            text_value,
        ]

        metadata = {
            "task_id": f"jupyter-ai-{msg_id}",
            "message_id": msg_id,
            "plugin_id": self.plugin_id,
            "channel": "jupyter_notebook_ai",
            "source": "jupyter_notebook_ui",
            "role": "user",
            "actor_role": "user_input",
            "event_origin": "jupyter_notebook_ui",
            "assistant_should_claim_action": False,
            "user_id": user_label,
            "user_identity": user_label,
            "text": text_value,
        }

        last_error: Exception | None = None
        for attempt in range(_AI_PUSH_RETRY_COUNT):
            try:
                self.ctx.push_message(
                    source=self.plugin_id,
                    message_type="proactive_notification",
                    description="Jupyter AI 用户消息",
                    priority=max(int(self._push_priority), 6),
                    content="\n".join(content_lines),
                    metadata=metadata,
                    target_lanlan=resolved_target or None,
                    fast_mode=True,
                )
                return Ok(
                    {
                        "accepted": True,
                        "message_id": msg_id,
                        "user_identity": user_label,
                        "target_lanlan": resolved_target,
                    }
                )
            except Exception as exc:
                last_error = exc
                if attempt < (_AI_PUSH_RETRY_COUNT - 1):
                    await asyncio.sleep(_AI_PUSH_RETRY_DELAY_SEC)

        return Err(SdkError(f"Failed to push AI message: {last_error}"))

    async def _execute_code(
        self,
        notebook_key: str,
        source: str,
        timeout_sec: float,
        on_stream: Any = None,
    ) -> dict[str, Any]:
        ensure_started = time.monotonic()
        runtime = await self._ensure_kernel(notebook_key)
        ensure_kernel_ms = int((time.monotonic() - ensure_started) * 1000)

        lock_wait_started = time.monotonic()
        async with runtime.lock:
            kernel_lock_wait_ms = int((time.monotonic() - lock_wait_started) * 1000)
            client = runtime.client
            manager = runtime.manager
            runtime.last_used_monotonic = time.monotonic()

            prepare_started = time.monotonic()
            prepared_source = self._prepare_kernel_source(source)
            prepare_source_ms = int((time.monotonic() - prepare_started) * 1000)

            dispatch_started = time.monotonic()
            msg_id = client.execute(prepared_source, stop_on_error=False, store_history=True)
            dispatch_send_ms = int((time.monotonic() - dispatch_started) * 1000)

            started_at = datetime.now(timezone.utc).isoformat()
            start = time.monotonic()
            outputs: list[dict[str, Any]] = []
            stdout_chunks: list[str] = []
            stderr_chunks: list[str] = []
            execution_count: int | None = None
            status = "ok"
            first_parent_msg_at: float | None = None
            first_payload_msg_at: float | None = None
            first_stream_msg_at: float | None = None
            message_count = 0

            while True:
                elapsed = time.monotonic() - start
                remaining = timeout_sec - elapsed
                if remaining <= 0:
                    status = "timeout"
                    break

                try:
                    msg = await client.get_iopub_msg(timeout=remaining)
                except asyncio.TimeoutError:
                    status = "timeout"
                    break
                except Exception:
                    status = "timeout"
                    break

                if str(((msg.get("parent_header") or {}).get("msg_id", ""))) != msg_id:
                    continue

                message_count += 1
                now_msg = time.monotonic()
                if first_parent_msg_at is None:
                    first_parent_msg_at = now_msg

                msg_type = str(msg.get("msg_type", ""))
                content = msg.get("content") if isinstance(msg.get("content"), dict) else {}

                if msg_type == "status":
                    if str(content.get("execution_state", "")) == "idle":
                        break
                    continue

                if msg_type == "execute_input":
                    value = content.get("execution_count")
                    if isinstance(value, int):
                        execution_count = value
                    continue

                if msg_type == "stream":
                    if first_payload_msg_at is None:
                        first_payload_msg_at = now_msg
                    if first_stream_msg_at is None:
                        first_stream_msg_at = now_msg
                    stream_name = str(content.get("name", "stdout"))
                    stream_text = self._sanitize_text(content.get("text", ""))
                    outputs.append(
                        {
                            "output_type": "stream",
                            "name": stream_name,
                            "text": stream_text,
                        }
                    )
                    if stream_name == "stderr":
                        stderr_chunks.append(stream_text)
                    else:
                        stdout_chunks.append(stream_text)
                    if callable(on_stream):
                        try:
                            maybe_awaitable = on_stream(stream_name, stream_text, int((time.monotonic() - start) * 1000))
                            if asyncio.iscoroutine(maybe_awaitable):
                                await maybe_awaitable
                        except Exception:
                            pass
                    continue

                if msg_type in {"execute_result", "display_data"}:
                    if first_payload_msg_at is None:
                        first_payload_msg_at = now_msg
                    data_obj = content.get("data") if isinstance(content.get("data"), dict) else {}
                    md_obj = content.get("metadata") if isinstance(content.get("metadata"), dict) else {}
                    out: dict[str, Any] = {
                        "output_type": msg_type,
                        "data": self._sanitize_json_value(data_obj),
                        "metadata": self._sanitize_json_value(md_obj),
                    }
                    value = content.get("execution_count")
                    if isinstance(value, int):
                        execution_count = value
                        out["execution_count"] = value
                    outputs.append(out)
                    continue

                if msg_type == "error":
                    if first_payload_msg_at is None:
                        first_payload_msg_at = now_msg
                    status = "error"
                    traceback_obj_raw = content.get("traceback")
                    traceback_obj: list[Any] = traceback_obj_raw if isinstance(traceback_obj_raw, list) else []
                    outputs.append(
                        {
                            "output_type": "error",
                            "ename": self._sanitize_text(content.get("ename", "Error")),
                            "evalue": self._sanitize_text(content.get("evalue", "")),
                            "traceback": [self._sanitize_text(x) for x in traceback_obj],
                        }
                    )
                    continue

            loop_ended_at = time.monotonic()
            shell_started = time.monotonic()
            shell_status = await self._read_shell_status(client, msg_id, timeout=1.0)
            shell_status_ms = int((time.monotonic() - shell_started) * 1000)
            if shell_status == "error":
                status = "error"
            elif shell_status == "ok" and status == "ok":
                status = "ok"

            if status == "timeout":
                try:
                    await manager.interrupt_kernel()
                except Exception:
                    pass
                outputs.append(
                    {
                        "output_type": "error",
                        "ename": "TimeoutError",
                        "evalue": f"Execution timed out after {timeout_sec:.1f}s",
                        "traceback": [],
                    }
                )

            finalized_at = time.monotonic()
            finished_at = datetime.now(timezone.utc).isoformat()
            duration_ms = int((finalized_at - start) * 1000)

            first_parent_msg_ms = int((first_parent_msg_at - start) * 1000) if first_parent_msg_at is not None else None
            first_payload_msg_ms = int((first_payload_msg_at - start) * 1000) if first_payload_msg_at is not None else None
            first_stream_msg_ms = int((first_stream_msg_at - start) * 1000) if first_stream_msg_at is not None else None
            execute_to_idle_ms = int((loop_ended_at - start) * 1000)
            post_idle_finalize_ms = int((finalized_at - loop_ended_at) * 1000)

            kernel_stage_ms = {
                "ensure_kernel_ms": ensure_kernel_ms,
                "kernel_lock_wait_ms": kernel_lock_wait_ms,
                "prepare_source_ms": prepare_source_ms,
                "dispatch_send_ms": dispatch_send_ms,
                "pre_dispatch_ms": ensure_kernel_ms + kernel_lock_wait_ms + prepare_source_ms + dispatch_send_ms,
                "first_parent_msg_ms": first_parent_msg_ms,
                "first_payload_msg_ms": first_payload_msg_ms,
                "first_stream_msg_ms": first_stream_msg_ms,
                "execute_to_idle_ms": execute_to_idle_ms,
                "shell_status_ms": shell_status_ms,
                "post_idle_finalize_ms": post_idle_finalize_ms,
                "message_count": message_count,
                "total_ms": duration_ms,
            }

            return {
                "success": status == "ok",
                "status": status,
                "outputs": self._sanitize_json_value(outputs),
                "execution_count": execution_count,
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_ms": duration_ms,
                "stdout_text": self._sanitize_text("".join(stdout_chunks)),
                "stderr_text": self._sanitize_text("".join(stderr_chunks)),
                "kernel_stage_ms": self._sanitize_json_value(kernel_stage_ms),
            }

    def _prepare_kernel_source(self, source: str) -> str:
        # Keep per-cell execution prelude tiny and idempotent.
        # Heavy imports/config are executed only once per kernel process.
        safe_source = self._sanitize_text(source)
        prelude = (
            "import os as __neko_os\n"
            "import sys as __neko_sys\n"
            "import builtins as __neko_builtins\n"
            "def __neko_clean_text(__neko_value):\n"
            "    __neko_text = '' if __neko_value is None else str(__neko_value)\n"
            "    if not __neko_text:\n"
            "        return ''\n"
            "    __neko_chars = []\n"
            "    for __neko_ch in __neko_text:\n"
            "        __neko_code = ord(__neko_ch)\n"
            "        if 0xD800 <= __neko_code <= 0xDFFF:\n"
            "            __neko_chars.append('\\ufffd')\n"
            "        else:\n"
            "            __neko_chars.append(__neko_ch)\n"
            "    return ''.join(__neko_chars)\n"
            "def __neko_wrap_stream_write(__neko_stream):\n"
            "    try:\n"
            "        __neko_orig_write = __neko_stream.write\n"
            "    except Exception:\n"
            "        return\n"
            "    if not callable(__neko_orig_write):\n"
            "        return\n"
            "    def __neko_safe_write(__neko_data):\n"
            "        __neko_payload = __neko_clean_text(__neko_data)\n"
            "        try:\n"
            "            return __neko_orig_write(__neko_payload)\n"
            "        except UnicodeEncodeError:\n"
            "            return __neko_orig_write(__neko_payload.encode('utf-8', 'replace').decode('utf-8', 'replace'))\n"
            "    try:\n"
            "        __neko_stream.write = __neko_safe_write\n"
            "    except Exception:\n"
            "        pass\n"
            "def __neko_install_safe_print():\n"
            "    try:\n"
            "        __neko_orig_print = __neko_builtins.print\n"
            "    except Exception:\n"
            "        return\n"
            "    if not callable(__neko_orig_print):\n"
            "        return\n"
            "    def __neko_safe_print(*__neko_args, **__neko_kwargs):\n"
            "        __neko_safe_args = tuple(__neko_clean_text(__x) for __x in __neko_args)\n"
            "        try:\n"
            "            return __neko_orig_print(*__neko_safe_args, **__neko_kwargs)\n"
            "        except UnicodeEncodeError:\n"
            "            __neko_reencoded = tuple(__x.encode('utf-8', 'replace').decode('utf-8', 'replace') for __x in __neko_safe_args)\n"
            "            return __neko_orig_print(*__neko_reencoded, **__neko_kwargs)\n"
            "    try:\n"
            "        __neko_builtins.print = __neko_safe_print\n"
            "    except Exception:\n"
            "        pass\n"
            "try:\n"
            "    if hasattr(__neko_sys.stdout, 'reconfigure'):\n"
            "        __neko_sys.stdout.reconfigure(encoding='utf-8', errors='replace')\n"
            "    if hasattr(__neko_sys.stderr, 'reconfigure'):\n"
            "        __neko_sys.stderr.reconfigure(encoding='utf-8', errors='replace')\n"
            "except Exception:\n"
            "    pass\n"
            "try:\n"
            "    __neko_wrap_stream_write(__neko_sys.stdout)\n"
            "    __neko_wrap_stream_write(__neko_sys.stderr)\n"
            "except Exception:\n"
            "    pass\n"
            "__neko_install_safe_print()\n"
            "if '__neko_runtime_once__' not in globals():\n"
            "    __neko_runtime_once__ = True\n"
            "    try:\n"
            "        __neko_os.environ.setdefault('PYTHONUTF8', '1')\n"
            "        __neko_os.environ.setdefault('PYTHONIOENCODING', 'utf-8:replace')\n"
            "        __neko_os.environ.setdefault('MPLBACKEND', 'module://matplotlib_inline.backend_inline')\n"
            "        __neko_os.environ.setdefault('PLOTLY_RENDERER', 'plotly_mimetype')\n"
            "    except Exception:\n"
            "        pass\n"
            "    try:\n"
            "        import matplotlib as __neko_mpl\n"
            "        try:\n"
            "            __neko_mpl.use('module://matplotlib_inline.backend_inline', force=True)\n"
            "        except Exception:\n"
            "            try:\n"
            "                __neko_mpl.use('Agg', force=True)\n"
            "            except Exception:\n"
            "                pass\n"
            "        try:\n"
            "            __neko_mpl.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Noto Sans CJK SC', 'PingFang SC', 'Arial Unicode MS', 'DejaVu Sans']\n"
            "            __neko_mpl.rcParams['axes.unicode_minus'] = False\n"
            "        except Exception:\n"
            "            pass\n"
            "    except Exception:\n"
            "        pass\n"
            "    try:\n"
            "        import plotly.io as __neko_pio\n"
            "        __neko_pio.renderers.default = 'plotly_mimetype'\n"
            "    except Exception:\n"
            "        pass\n"
        )
        return prelude + "\n" + safe_source

    async def _read_shell_status(self, client: Any, msg_id: str, timeout: float = 1.0) -> str | None:
        deadline = time.monotonic() + max(0.1, timeout)
        while time.monotonic() < deadline:
            remain = deadline - time.monotonic()
            try:
                msg = await client.get_shell_msg(timeout=remain)
            except asyncio.TimeoutError:
                return None
            except Exception:
                return None

            if str(((msg.get("parent_header") or {}).get("msg_id", ""))) != msg_id:
                continue

            content = msg.get("content") if isinstance(msg.get("content"), dict) else {}
            status_obj = content.get("status")
            return str(status_obj) if status_obj is not None else None

        return None

    def _execute_code_subprocess_fallback(self, source: str, timeout_sec: float, reason: str) -> dict[str, Any]:
        python_exec = self._venv_python_exec or (str(self._venv_python) if self._venv_python is not None else sys.executable)
        started = time.monotonic()
        started_at = datetime.now(timezone.utc).isoformat()
        safe_source = self._sanitize_text(source)

        marker_token = uuid.uuid4().hex
        rich_marker_start = f"__NEKO_RICH_OUTPUT_BEGIN__{marker_token}__"
        rich_marker_end = f"__NEKO_RICH_OUTPUT_END__{marker_token}__"

        wrapped_source = (
            "import json as __neko_json\n"
            "import base64 as __neko_base64\n"
            "import io as __neko_io\n"
            "import os as __neko_os\n"
            "import sys as __neko_sys\n"
            "import builtins as __neko_builtins\n"
            "def __neko_clean_text(__neko_value):\n"
            "    __neko_text = '' if __neko_value is None else str(__neko_value)\n"
            "    if not __neko_text:\n"
            "        return ''\n"
            "    __neko_chars = []\n"
            "    for __neko_ch in __neko_text:\n"
            "        __neko_code = ord(__neko_ch)\n"
            "        if 0xD800 <= __neko_code <= 0xDFFF:\n"
            "            __neko_chars.append('\\ufffd')\n"
            "        else:\n"
            "            __neko_chars.append(__neko_ch)\n"
            "    return ''.join(__neko_chars)\n"
            "def __neko_wrap_stream_write(__neko_stream):\n"
            "    try:\n"
            "        __neko_orig_write = __neko_stream.write\n"
            "    except Exception:\n"
            "        return\n"
            "    if not callable(__neko_orig_write):\n"
            "        return\n"
            "    def __neko_safe_write(__neko_data):\n"
            "        __neko_payload = __neko_clean_text(__neko_data)\n"
            "        try:\n"
            "            return __neko_orig_write(__neko_payload)\n"
            "        except UnicodeEncodeError:\n"
            "            return __neko_orig_write(__neko_payload.encode('utf-8', 'replace').decode('utf-8', 'replace'))\n"
            "    try:\n"
            "        __neko_stream.write = __neko_safe_write\n"
            "    except Exception:\n"
            "        pass\n"
            "def __neko_install_safe_print():\n"
            "    try:\n"
            "        __neko_orig_print = __neko_builtins.print\n"
            "    except Exception:\n"
            "        return\n"
            "    if not callable(__neko_orig_print):\n"
            "        return\n"
            "    def __neko_safe_print(*__neko_args, **__neko_kwargs):\n"
            "        __neko_safe_args = tuple(__neko_clean_text(__x) for __x in __neko_args)\n"
            "        try:\n"
            "            return __neko_orig_print(*__neko_safe_args, **__neko_kwargs)\n"
            "        except UnicodeEncodeError:\n"
            "            __neko_reencoded = tuple(__x.encode('utf-8', 'replace').decode('utf-8', 'replace') for __x in __neko_safe_args)\n"
            "            return __neko_orig_print(*__neko_reencoded, **__neko_kwargs)\n"
            "    try:\n"
            "        __neko_builtins.print = __neko_safe_print\n"
            "    except Exception:\n"
            "        pass\n"
            "try:\n"
            "    if hasattr(__neko_sys.stdout, 'reconfigure'):\n"
            "        __neko_sys.stdout.reconfigure(encoding='utf-8', errors='replace')\n"
            "    if hasattr(__neko_sys.stderr, 'reconfigure'):\n"
            "        __neko_sys.stderr.reconfigure(encoding='utf-8', errors='replace')\n"
            "except Exception:\n"
            "    pass\n"
            "try:\n"
            "    __neko_wrap_stream_write(__neko_sys.stdout)\n"
            "    __neko_wrap_stream_write(__neko_sys.stderr)\n"
            "except Exception:\n"
            "    pass\n"
            "__neko_install_safe_print()\n"
            "try:\n"
            "    __neko_os.environ.setdefault('PYTHONUTF8', '1')\n"
            "    __neko_os.environ.setdefault('PYTHONIOENCODING', 'utf-8:replace')\n"
            "except Exception:\n"
            "    pass\n"
            "def __neko_make_json_safe(__neko_obj):\n"
            "    if __neko_obj is None or isinstance(__neko_obj, (str, int, float, bool)):\n"
            "        return __neko_obj\n"
            "    try:\n"
            "        if hasattr(__neko_obj, 'to_plotly_json') and callable(__neko_obj.to_plotly_json):\n"
            "            return __neko_make_json_safe(__neko_obj.to_plotly_json())\n"
            "    except Exception:\n"
            "        pass\n"
            "    if isinstance(__neko_obj, dict):\n"
            "        return {str(__k): __neko_make_json_safe(__v) for __k, __v in __neko_obj.items()}\n"
            "    if isinstance(__neko_obj, (list, tuple, set)):\n"
            "        return [__neko_make_json_safe(__x) for __x in __neko_obj]\n"
            "    try:\n"
            "        if hasattr(__neko_obj, 'tolist') and callable(__neko_obj.tolist):\n"
            "            return __neko_make_json_safe(__neko_obj.tolist())\n"
            "    except Exception:\n"
            "        pass\n"
            "    try:\n"
            "        if hasattr(__neko_obj, 'item') and callable(__neko_obj.item):\n"
            "            return __neko_make_json_safe(__neko_obj.item())\n"
            "    except Exception:\n"
            "        pass\n"
            "    return __neko_clean_text(__neko_obj)\n"
            "def __neko_dump_rich_payload(__neko_payload):\n"
            "    try:\n"
            "        from plotly.utils import PlotlyJSONEncoder as __neko_plotly_encoder\n"
            "        return __neko_json.dumps(__neko_payload, cls=__neko_plotly_encoder, ensure_ascii=True)\n"
            "    except Exception:\n"
            "        return __neko_json.dumps(__neko_make_json_safe(__neko_payload), ensure_ascii=True)\n"
            "__neko_plotly_specs = []\n"
            "__neko_matplotlib_png = []\n"
            "try:\n"
            "    import plotly.io as __neko_pio\n"
            "    __neko_orig_pio_show = __neko_pio.show\n"
            "\n"
            "    def __neko_capture_plotly_show(fig=None, *args, **kwargs):\n"
            "        if fig is not None:\n"
            "            try:\n"
            "                if hasattr(fig, 'to_plotly_json'):\n"
            "                    __neko_plotly_specs.append(fig.to_plotly_json())\n"
            "                    return None\n"
            "            except Exception:\n"
            "                pass\n"
            "        try:\n"
            "            return __neko_orig_pio_show(fig, *args, **kwargs)\n"
            "        except Exception:\n"
            "            return None\n"
            "\n"
            "    __neko_pio.show = __neko_capture_plotly_show\n"
            "\n"
            "    try:\n"
            "        import plotly.basedatatypes as __neko_bdt\n"
            "        import plotly.graph_objects as __neko_go\n"
            "\n"
            "        def __neko_figure_show(self, *args, **kwargs):\n"
            "            return __neko_capture_plotly_show(self, *args, **kwargs)\n"
            "\n"
            "        __neko_bdt.BaseFigure.show = __neko_figure_show\n"
            "        __neko_go.Figure.show = __neko_figure_show\n"
            "    except Exception:\n"
            "        pass\n"
            "except Exception:\n"
            "    pass\n"
            "\n"
            "try:\n"
            "    import matplotlib\n"
            "    matplotlib.use('Agg', force=True)\n"
            "    import matplotlib.pyplot as __neko_plt\n"
            "    try:\n"
            "        matplotlib.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Noto Sans CJK SC', 'PingFang SC', 'Arial Unicode MS', 'DejaVu Sans']\n"
            "        matplotlib.rcParams['axes.unicode_minus'] = False\n"
            "    except Exception:\n"
            "        pass\n"
            "\n"
            "    def __neko_capture_matplotlib_show(*args, **kwargs):\n"
            "        figs = [__neko_plt.figure(num) for num in __neko_plt.get_fignums()]\n"
            "        for fig in figs:\n"
            "            buf = __neko_io.BytesIO()\n"
            "            fig.savefig(buf, format='png', bbox_inches='tight')\n"
            "            __neko_matplotlib_png.append(__neko_base64.b64encode(buf.getvalue()).decode('ascii'))\n"
            "            buf.close()\n"
            "        __neko_plt.close('all')\n"
            "\n"
            "    __neko_plt.show = __neko_capture_matplotlib_show\n"
            "except Exception:\n"
            "    pass\n"
            "\n"
            "__neko_globals = {'__name__': '__main__'}\n"
            "try:\n"
            f"    exec({json.dumps(safe_source, ensure_ascii=False)}, __neko_globals, __neko_globals)\n"
            "finally:\n"
            f"    print('{rich_marker_start}')\n"
            "    try:\n"
            "        print(__neko_dump_rich_payload({'plotly': __neko_plotly_specs, 'matplotlib_png': __neko_matplotlib_png}))\n"
            "    except Exception:\n"
            "        print('{}')\n"
            f"    print('{rich_marker_end}')\n"
        )

        stdout_text = ""
        stderr_text = ""
        status = "ok"
        success = True
        outputs: list[dict[str, Any]] = []

        try:
            env = self._build_utf8_runtime_env(
                {
                    "PLOTLY_RENDERER": "json",
                    "BROWSER": "false",
                    "MPLBACKEND": "Agg",
                }
            )

            proc = subprocess.run(
                [python_exec, "-"],
                input=wrapped_source,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=max(1.0, float(timeout_sec)),
                check=False,
                env=env,
            )
            stdout_text = self._sanitize_text(proc.stdout or "")
            stderr_text = self._sanitize_text(proc.stderr or "")

            if stdout_text:
                begin = stdout_text.find(rich_marker_start)
                end = stdout_text.find(rich_marker_end, begin + len(rich_marker_start)) if begin >= 0 else -1
                if begin >= 0 and end > begin:
                    json_text = stdout_text[begin + len(rich_marker_start):end].strip()
                    clean_stdout = (stdout_text[:begin] + stdout_text[end + len(rich_marker_end):]).strip()
                    stdout_text = self._sanitize_text(clean_stdout)
                    try:
                        payload = json.loads(json_text) if json_text else {}
                    except Exception:
                        payload = {}

                    if isinstance(payload, dict):
                        plotly_payload = payload.get("plotly")
                        if isinstance(plotly_payload, list):
                            for spec in plotly_payload:
                                if isinstance(spec, dict):
                                    outputs.append(
                                        {
                                            "output_type": "display_data",
                                            "data": {"application/vnd.plotly.v1+json": self._sanitize_json_value(spec)},
                                            "metadata": {},
                                        }
                                    )

                        matplotlib_payload = payload.get("matplotlib_png")
                        if isinstance(matplotlib_payload, list):
                            for image_b64 in matplotlib_payload:
                                if not isinstance(image_b64, str) or not image_b64.strip():
                                    continue
                                outputs.append(
                                    {
                                        "output_type": "display_data",
                                        "data": {"image/png": self._sanitize_text(image_b64).strip()},
                                        "metadata": {},
                                    }
                                )

                    elif isinstance(payload, list):
                        for spec in payload:
                            if isinstance(spec, dict):
                                outputs.append(
                                    {
                                        "output_type": "display_data",
                                        "data": {"application/vnd.plotly.v1+json": self._sanitize_json_value(spec)},
                                        "metadata": {},
                                    }
                                )

            if stdout_text:
                outputs.append({"output_type": "stream", "name": "stdout", "text": stdout_text})
            if stderr_text:
                outputs.append({"output_type": "stream", "name": "stderr", "text": stderr_text})
            if proc.returncode != 0:
                success = False
                status = "error"
                outputs.append(
                    {
                        "output_type": "error",
                        "ename": "SubprocessError",
                        "evalue": f"Exit code {proc.returncode}",
                        "traceback": [],
                    }
                )
        except subprocess.TimeoutExpired:
            success = False
            status = "timeout"
            outputs.append(
                {
                    "output_type": "error",
                    "ename": "TimeoutError",
                    "evalue": f"Execution timed out after {timeout_sec:.1f}s",
                    "traceback": [],
                }
            )
        except Exception as exc:
            success = False
            status = "error"
            outputs.append(
                {
                    "output_type": "error",
                    "ename": "FallbackExecutionError",
                    "evalue": str(exc),
                    "traceback": [],
                }
            )

        finished_at = datetime.now(timezone.utc).isoformat()
        duration_ms = int((time.monotonic() - started) * 1000)
        if not success and reason:
            stderr_text = (stderr_text + "\n" if stderr_text else "") + f"[kernel_fallback_reason] {reason}"

        return {
            "success": success,
            "status": status,
            "outputs": self._sanitize_json_value(outputs),
            "execution_count": None,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": duration_ms,
            "stdout_text": self._sanitize_text(stdout_text),
            "stderr_text": self._sanitize_text(stderr_text),
            "backend": "subprocess",
        }

    async def _ensure_kernel(self, notebook_key: str) -> KernelRuntime:
        if AsyncKernelManager is None or not self._runtime_ready:
            reason = self._runtime_error or "jupyter runtime is not available in isolated env"
            raise SdkError(reason)

        runtime_key = self._kernel_key(notebook_key)
        async with self._kernel_lock:
            existing = self._kernels.get(runtime_key)
            if existing is not None:
                return existing

            manager = AsyncKernelManager(kernel_name=self._default_kernel_name)
            if self._venv_python is not None and self._venv_python.exists():
                manager.kernel_cmd = [
                    self._venv_python_exec or str(self._venv_python),
                    "-m",
                    "ipykernel_launcher",
                    "-f",
                    "{connection_file}",
                ]
            await manager.start_kernel(
                env=self._build_utf8_runtime_env(
                    {
                        "MPLBACKEND": "module://matplotlib_inline.backend_inline",
                        "PLOTLY_RENDERER": "plotly_mimetype",
                    }
                )
            )
            client = manager.client()
            client.start_channels()
            try:
                await client.wait_for_ready(timeout=45)
            except Exception as exc:
                try:
                    client.stop_channels()
                except Exception:
                    pass
                try:
                    await manager.shutdown_kernel(now=True)
                except Exception:
                    pass
                raise SdkError(f"Kernel is not ready: {exc}") from exc

            runtime = KernelRuntime(manager=manager, client=client)
            self._kernels[runtime_key] = runtime
            return runtime

    async def _shutdown_kernel(self, notebook_key: str) -> bool:
        runtime_key = self._kernel_key(notebook_key)
        async with self._kernel_lock:
            runtime = self._kernels.pop(runtime_key, None)
        if runtime is None:
            return False
        await self._shutdown_runtime(runtime)
        return True

    async def _restart_kernel_for_notebook(self, notebook_key: str) -> None:
        runtime_key = self._kernel_key(notebook_key)
        async with self._kernel_lock:
            runtime = self._kernels.get(runtime_key)

        if runtime is not None:
            async with runtime.lock:
                try:
                    await runtime.manager.restart_kernel(now=True)
                    await runtime.client.wait_for_ready(timeout=45)
                    runtime.last_used_monotonic = time.monotonic()
                    return
                except Exception:
                    pass

        await self._shutdown_kernel(notebook_key)
        await self._ensure_kernel(notebook_key)

    def _kernel_key(self, notebook_key: str) -> str:
        if self._shared_kernel:
            return "__shared_kernel__"
        return str(notebook_key or "")

    async def _shutdown_runtime(self, runtime: KernelRuntime) -> None:
        try:
            runtime.client.stop_channels()
        except Exception:
            pass
        try:
            await runtime.manager.shutdown_kernel(now=True)
        except Exception:
            pass

    async def _shutdown_all_kernels(self) -> None:
        async with self._kernel_lock:
            items = list(self._kernels.values())
            self._kernels.clear()
        for runtime in items:
            await self._shutdown_runtime(runtime)

    def _start_event_worker(self) -> None:
        self._event_stop.clear()
        self._event_wake.clear()
        thread = self._event_thread
        if thread is not None and thread.is_alive():
            return
        self._event_thread = threading.Thread(target=self._event_worker_loop, daemon=True, name="jupyter-event-pusher")
        self._event_thread.start()

    def _event_worker_loop(self) -> None:
        while not self._event_stop.is_set():
            self._event_wake.wait(timeout=0.8)
            self._event_wake.clear()

            while True:
                with self._event_lock:
                    event = self._event_queue.popleft() if self._event_queue else None
                if event is None:
                    break
                self._push_event(event)

    def _enqueue_event(
        self,
        *,
        event_type: str,
        summary: str,
        metadata: dict[str, Any] | None = None,
        target_lanlan: str = "",
        priority: int | None = None,
    ) -> None:
        event_ts = datetime.now(timezone.utc).isoformat()
        metadata_payload = dict(metadata or {})
        metadata_payload.setdefault("timestamp", event_ts)
        metadata_payload.setdefault("event_type", str(event_type))
        metadata_payload.setdefault("actor_role", "user_action")
        metadata_payload.setdefault("event_origin", "jupyter_notebook_ui")
        metadata_payload.setdefault("assistant_should_claim_action", False)

        event = {
            "id": uuid.uuid4().hex[:12],
            "timestamp": event_ts,
            "event_type": str(event_type),
            "summary": str(summary),
            "metadata": metadata_payload,
            "target_lanlan": str(target_lanlan or "").strip(),
            "priority": int(self._push_priority if priority is None else priority),
            "pushed": False,
        }

        with self._event_lock:
            if len(self._event_queue) >= self._max_event_queue:
                self._event_queue.popleft()
            self._event_queue.append(event)
            self._recent_events.append(dict(event))

        self._event_wake.set()

    def _push_event(self, event: dict[str, Any]) -> None:
        event_type = str(event.get("event_type", "jupyter_event"))
        summary = str(event.get("summary", "Jupyter event"))
        metadata_obj = event.get("metadata")
        metadata: dict[str, Any] = dict(metadata_obj) if isinstance(metadata_obj, dict) else {}
        target_lanlan = str(event.get("target_lanlan", "")).strip()
        user_id = target_lanlan or str(metadata.get("user_id", "") or "主人").strip() or "主人"

        if (not self._enable_model_push) or (event_type in _MODEL_PUSH_BLOCKED_EVENT_TYPES):
            event["pushed"] = False
            event["push_skipped"] = "disabled" if not self._enable_model_push else "blocked_event_type"
            with self._event_lock:
                self._recent_events.append(dict(event))
            return

        notebook_path = str(metadata.get("notebook_path", "") or "").strip()
        status = str(metadata.get("status", "") or "").strip()

        if event_type == "cell_execution_output":
            content_lines = self._build_execution_push_lines(metadata, user_id=user_id)
        else:
            content_lines = [
                "Jupyter Notebook 操作通知",
                "系统提示：以下内容是用户在 Notebook 界面的操作记录，不是助手自我行为回放。",
                f"事件: {self._friendly_event_name(event_type)}",
            ]
            if notebook_path:
                content_lines.append(f"Notebook: {notebook_path}")
            if status:
                content_lines.append(f"状态: {status}")
            if summary:
                content_lines.append(f"摘要: {self._clip_text(summary, 180)}")

        push_error = ""
        for attempt in range(_EVENT_PUSH_RETRY_COUNT):
            try:
                self.ctx.push_message(
                    source=self.plugin_id,
                    message_type="proactive_notification",
                    description=f"Jupyter事件: {event_type}",
                    priority=int(event.get("priority", self._push_priority)),
                    content="\n".join(content_lines),
                    metadata={
                        "task_id": f"jupyter-{event.get('id')}",
                        "event_id": event.get("id"),
                        "event_timestamp": event.get("timestamp"),
                        "event_type": event_type,
                        "summary": summary,
                        "plugin_id": self.plugin_id,
                        "notebook_path": notebook_path,
                        "status": status,
                        "user_id": user_id,
                        "actor_role": "user_action",
                        "event_origin": "jupyter_notebook_ui",
                        "assistant_should_claim_action": False,
                    },
                    target_lanlan=target_lanlan or None,
                    fast_mode=True,
                )
                event["pushed"] = True
                push_error = ""
                break
            except Exception as exc:
                event["pushed"] = False
                push_error = str(exc)
                if attempt < (_EVENT_PUSH_RETRY_COUNT - 1):
                    time.sleep(_EVENT_PUSH_RETRY_DELAY_SEC)

        if push_error:
            event["push_error"] = push_error
            try:
                self.logger.warning("Failed to push Jupyter event after retries: {}", push_error)
            except Exception:
                pass

        with self._event_lock:
            self._recent_events.append(dict(event))

    @staticmethod
    def _friendly_event_name(event_type: str) -> str:
        mapping = {
            "notebook_created": "已创建 Notebook",
            "notebook_saved": "已保存 Notebook",
            "cell_added": "已新增单元格",
            "cell_deleted": "已删除单元格",
            "cell_execution_status": "单元格执行状态",
            "cell_execution_output": "单元格执行结果",
            "kernel_restarted": "内核已重启",
            "kernel_shutdown": "内核已关闭",
        }
        key = str(event_type or "").strip()
        return mapping.get(key, key or "未知事件")

    def _build_execution_push_lines(self, metadata: dict[str, Any], user_id: str) -> list[str]:
        notebook_path = str(metadata.get("notebook_path", "") or "").strip()
        success = bool(metadata.get("success", False))
        status = str(metadata.get("status", "") or "").strip()
        code_content = str(metadata.get("code_content", "") or "")
        output_text = str(metadata.get("output_text", "") or "")
        has_visual_output = bool(metadata.get("has_visual_output", False))

        lines = [
            "Jupyter Notebook 执行结果通知",
            f"用户标识: {user_id}",
            "系统提示：这是用户手动触发的运行结果，不是助手自己执行了代码。",
        ]
        if notebook_path:
            lines.append(f"Notebook: {notebook_path}")
        if status:
            lines.append(f"执行状态: {status}")

        lines.extend(["", "完整代码:", code_content or "(空代码)"])

        if has_visual_output and not output_text.strip():
            lines.extend([
                "",
                "运行结果:",
                "检测到图表/图像输出，文本结果不可直接推送；请基于上方完整代码进行解读并给予反馈。",
            ])
        else:
            lines.extend(["", "运行结果:", output_text or "(无文本输出)"])

        emotion_prompt = (
            "请给出真诚且具体的情绪价值反馈：先夸赞用户的努力与思路亮点，再给一句温暖祝福和可执行的下一步建议；避免机械重复“成功了”。"
            if success
            else "请先温柔安慰用户并肯定其投入，再用积极语气鼓励继续尝试，并给出一个可落地的排查建议；避免机械重复“失败了”。"
        )
        lines.extend(["", f"情绪提示词: {emotion_prompt}"])
        return lines

    def _resolve_target_lanlan(self, preferred: str, kwargs: dict[str, Any]) -> str:
        first = str(preferred or "").strip()
        if first:
            return first

        ctx_obj = kwargs.get("_ctx")
        if isinstance(ctx_obj, dict):
            for key in ("target_lanlan", "lanlan_name"):
                value = str(ctx_obj.get(key, "") or "").strip()
                if value:
                    return value

        for key in ("NEKO_TARGET_LANLAN", "NEKO_LANLAN_NAME", "NEKO_HER_NAME"):
            value = str(os.getenv(key, "") or "").strip()
            if value:
                return value

        return ""

    def _plugin_ui_url(self) -> str:
        origin = self._resolve_public_origin()
        return f"{origin}/plugin/{self.plugin_id}/ui/"

    def _resolve_public_origin(self) -> str:
        for key in ("NEKO_PLUGIN_SERVER_ORIGIN", "NEKO_USER_PLUGIN_SERVER_ORIGIN", "NEKO_SERVER_ORIGIN"):
            value = str(os.getenv(key, "") or "").strip().rstrip("/")
            if value.startswith("http://") or value.startswith("https://"):
                return value

        try:
            from config import USER_PLUGIN_SERVER_PORT

            port = int(USER_PLUGIN_SERVER_PORT)
            if 1 <= port <= 65535:
                return f"http://127.0.0.1:{port}"
        except Exception:
            pass

        try:
            env_port = int(str(os.getenv("NEKO_USER_PLUGIN_SERVER_PORT", "")).strip())
            if 1 <= env_port <= 65535:
                return f"http://127.0.0.1:{env_port}"
        except Exception:
            pass

        return "http://127.0.0.1:48916"

    def _list_notebook_files(self) -> list[dict[str, Any]]:
        self._workspace_root.mkdir(parents=True, exist_ok=True)
        items: list[dict[str, Any]] = []
        for path in sorted(self._workspace_root.rglob("*.ipynb")):
            try:
                rel = self._to_relative_notebook_path(path)
                stat = path.stat()
                items.append(
                    {
                        "path": rel,
                        "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                        "size": stat.st_size,
                    }
                )
            except Exception:
                continue
        return items

    @staticmethod
    def _cells_from_notebook(notebook: dict[str, Any]) -> list[dict[str, Any]]:
        cells_obj = notebook.get("cells")
        if not isinstance(cells_obj, list):
            return []
        cells: list[dict[str, Any]] = []
        for item in cells_obj:
            if isinstance(item, dict):
                cells.append(item)
        return cells

    def _resolve_notebook_path(self, notebook_path: str) -> Path:
        raw = str(notebook_path or "").strip().replace("\\", "/")
        if not raw:
            raw = _DEFAULT_NOTEBOOK_NAME
        if not raw.lower().endswith(".ipynb"):
            raw += ".ipynb"

        candidate = (self._workspace_root / raw).resolve()
        root = self._workspace_root.resolve()
        try:
            candidate.relative_to(root)
        except Exception as exc:
            raise SdkError("Notebook path must stay inside plugin workspace") from exc
        return candidate

    def _to_relative_notebook_path(self, path: Path) -> str:
        root = self._workspace_root.resolve()
        rel = path.resolve().relative_to(root)
        return str(rel).replace("\\", "/")

    def _build_default_notebook(self, title: str = "N.E.K.O Notebook") -> dict[str, Any]:
        return {
            "cells": [
                self._new_cell("markdown", f"# {title}\n\n欢迎使用 N.E.K.O Jupyter Notebook 插件。"),
                self._new_cell("code", "print('hello from N.E.K.O notebook plugin')"),
            ],
            "metadata": self._merge_notebook_metadata({"title": title}),
            "nbformat": 4,
            "nbformat_minor": 5,
        }

    def _new_cell(self, cell_type: str, source: str) -> dict[str, Any]:
        normalized_type = self._normalize_cell_type(cell_type)
        metadata = {
            "id": uuid.uuid4().hex[:12],
            "language": "python" if normalized_type == "code" else "markdown",
        }
        if normalized_type == "view":
            metadata[_VIEW_CELL_METADATA_KEY] = "view"
        cell: dict[str, Any] = {
            "cell_type": normalized_type,
            "metadata": metadata,
            "source": self._source_to_lines(source),
        }
        if normalized_type == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
        return cell

    def _read_notebook_text(self, path: Path) -> str:
        with self._notebook_lock:
            if not path.exists():
                notebook = self._build_default_notebook(title=path.stem)
                self._write_notebook(path, notebook)
            return path.read_text(encoding="utf-8", errors="replace")

    @staticmethod
    def _scan_json_string_end(raw: str, start_quote: int) -> int:
        i = max(0, int(start_quote)) + 1
        escaped = False
        while i < len(raw):
            ch = raw[i]
            if escaped:
                escaped = False
                i += 1
                continue
            if ch == "\\":
                escaped = True
                i += 1
                continue
            if ch == '"':
                return i + 1
            i += 1
        return -1

    @classmethod
    def _find_matching_bracket(cls, raw: str, start: int, open_ch: str, close_ch: str) -> int:
        i = int(start)
        if i < 0 or i >= len(raw) or raw[i] != open_ch:
            return -1

        depth = 0
        in_string = False
        escaped = False
        while i < len(raw):
            ch = raw[i]
            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
                i += 1
                continue

            if ch == '"':
                in_string = True
                i += 1
                continue

            if ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    return i
            i += 1

        return -1

    @classmethod
    def _scan_json_value_end(cls, raw: str, start: int) -> int:
        i = int(start)
        if i < 0 or i >= len(raw):
            return -1

        ch = raw[i]
        if ch == '"':
            return cls._scan_json_string_end(raw, i)
        if ch == "{":
            end = cls._find_matching_bracket(raw, i, "{", "}")
            return -1 if end < 0 else end + 1
        if ch == "[":
            end = cls._find_matching_bracket(raw, i, "[", "]")
            return -1 if end < 0 else end + 1

        while i < len(raw) and raw[i] not in {",", "}", "]"}:
            i += 1
        return i

    @classmethod
    def _extract_top_level_raw_value_from_object(cls, raw: str, key: str) -> str:
        i = 0
        while i < len(raw) and raw[i].isspace():
            i += 1
        if i >= len(raw) or raw[i] != "{":
            return ""
        i += 1

        while i < len(raw):
            while i < len(raw) and raw[i].isspace():
                i += 1
            if i >= len(raw):
                break
            if raw[i] == "}":
                break
            if raw[i] == ",":
                i += 1
                continue
            if raw[i] != '"':
                i += 1
                continue

            key_start = i
            key_end = cls._scan_json_string_end(raw, key_start)
            if key_end < 0:
                break
            try:
                parsed_key = str(json.loads(raw[key_start:key_end]))
            except Exception:
                parsed_key = ""

            i = key_end
            while i < len(raw) and raw[i].isspace():
                i += 1
            if i >= len(raw) or raw[i] != ":":
                continue
            i += 1
            while i < len(raw) and raw[i].isspace():
                i += 1
            val_start = i
            val_end = cls._scan_json_value_end(raw, val_start)
            if val_end < 0:
                break

            if parsed_key == key:
                return raw[val_start:val_end]
            i = val_end

        return ""

    def _scan_cells_ranges_from_text(self, text: str) -> list[tuple[int, int]]:
        marker = '"cells"'
        key_idx = text.find(marker)
        if key_idx < 0:
            raise SdkError("Invalid notebook: cells key missing")

        colon_idx = text.find(":", key_idx + len(marker))
        if colon_idx < 0:
            raise SdkError("Invalid notebook: cells key malformed")

        arr_start = colon_idx + 1
        while arr_start < len(text) and text[arr_start].isspace():
            arr_start += 1
        if arr_start >= len(text) or text[arr_start] != "[":
            raise SdkError("Invalid notebook: cells is not an array")

        arr_end = self._find_matching_bracket(text, arr_start, "[", "]")
        if arr_end < 0:
            raise SdkError("Invalid notebook: cells array not closed")

        ranges: list[tuple[int, int]] = []
        depth = 0
        in_string = False
        escaped = False
        obj_start = -1

        i = arr_start + 1
        while i < arr_end:
            ch = text[i]
            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
                i += 1
                continue

            if ch == '"':
                in_string = True
                i += 1
                continue

            if ch == "{":
                if depth == 0:
                    obj_start = i
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0 and obj_start >= 0:
                    ranges.append((obj_start, i + 1))
                    obj_start = -1
            i += 1

        return ranges

    def _extract_cell_outline_from_raw(self, raw: str, index: int, notebook_path: str) -> dict[str, Any]:
        metadata_raw = self._extract_top_level_raw_value_from_object(raw, "metadata")
        metadata_obj = self._dict_or_empty(self._sanitize_json_value(self._decode_json_value_safe(metadata_raw)))

        cell_type_raw = self._extract_top_level_raw_value_from_object(raw, "cell_type")
        cell_type_obj = self._decode_json_value_safe(cell_type_raw)
        cell_type = self._normalize_ui_cell_type_from_payload(cell_type_obj, metadata_obj)

        cell_id_raw = self._extract_top_level_raw_value_from_object(raw, "id")
        cell_id_obj = self._decode_json_value_safe(cell_id_raw)
        cell_id = str(metadata_obj.get("id") or cell_id_obj or "").strip() or uuid.uuid4().hex[:12]
        metadata_obj["id"] = cell_id

        source_raw = self._extract_top_level_raw_value_from_object(raw, "source")
        source_obj = self._decode_json_value_safe(source_raw)
        source_text = self._source_to_text(source_obj)
        source_lines = max(1, len(source_text.splitlines()) if source_text else 1)

        execution_count_raw = self._extract_top_level_raw_value_from_object(raw, "execution_count")
        execution_count_obj = self._decode_json_value_safe(execution_count_raw)
        execution_count = execution_count_obj if isinstance(execution_count_obj, int) else None

        outputs_raw = self._extract_top_level_raw_value_from_object(raw, "outputs")
        outputs_empty = outputs_raw.strip() in {"", "[]", "[ ]"}
        has_output = bool(outputs_raw) and not outputs_empty
        output_count = 1 if has_output else 0

        has_video = (
            ('"video/' in outputs_raw)
            or ('"video\\/' in outputs_raw)
            or ('"neko_media_kind": "video"' in outputs_raw)
            or ('"neko_media_kind":"video"' in outputs_raw)
            or ("<video" in outputs_raw.lower())
        )
        primary_media_mime = "video/mp4" if has_video else ""

        return {
            "index": int(index),
            "id": cell_id,
            "cell_type": cell_type,
            "metadata": metadata_obj,
            "source": source_text,
            "outputs": [],
            "output_text": "",
            "execution_count": execution_count,
            "outline_only": True,
            "details_loaded": False,
            "source_line_count": source_lines,
            "has_output": has_output,
            "output_count": output_count,
            "has_video_output": has_video,
            "primary_media_mime": primary_media_mime,
            "primary_media_size_bytes": 0,
            "notebook_path": notebook_path,
        }

    @staticmethod
    def _decode_json_value_safe(raw_value: str) -> Any:
        value = str(raw_value or "").strip()
        if not value:
            return None
        try:
            return json.loads(value)
        except Exception:
            return None

    def _serialize_notebook_outline_streaming_for_ui(self, path: Path, notebook_path: str) -> dict[str, Any]:
        text = self._read_notebook_text(path)
        ranges = self._scan_cells_ranges_from_text(text)
        cells: list[dict[str, Any]] = []

        for index, (start, end) in enumerate(ranges):
            raw = text[start:end]
            outline = self._extract_cell_outline_from_raw(raw, index, notebook_path)
            outline["source"] = ""
            outline["details_loaded"] = False
            outline["outline_only"] = True
            cells.append(outline)

        return {
            "path": notebook_path,
            "cells": cells,
            "total_cell_count": len(cells),
            "loaded_cell_offset": 0,
            "loaded_cell_count": len(cells),
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 5,
            "outline_only": True,
        }

    def _serialize_single_cell_detail_streaming_for_ui(self, path: Path, notebook_path: str, cell_index: int) -> dict[str, Any]:
        text = self._read_notebook_text(path)
        ranges = self._scan_cells_ranges_from_text(text)
        idx = self._coerce_int(cell_index, 0, minimum=0, maximum=max(0, len(ranges) - 1))

        if idx >= len(ranges):
            raise SdkError("Cell index out of range")

        start, end = ranges[idx]
        raw = text[start:end]
        outline = self._extract_cell_outline_from_raw(raw, idx, notebook_path)

        # For heavy media cells, defer payload extraction until explicit user action (play/load).
        has_heavy_media = bool(outline.get("has_video_output")) or (len(raw) >= _LAZY_MEDIA_THRESHOLD_BYTES)
        if has_heavy_media:
            outputs: list[dict[str, Any]] = []
            if bool(outline.get("has_output")):
                outputs.append(
                    {
                        "output_type": "display_data",
                        "data": {},
                        "metadata": {
                            "neko_media_backend_ref": {
                                "notebook_path": notebook_path,
                                "cell_index": idx,
                            },
                            "neko_media_mime": str(outline.get("primary_media_mime") or "video/mp4"),
                            "neko_media_size_bytes": int(outline.get("primary_media_size_bytes") or 0),
                        },
                    }
                )

            cell = {
                "index": idx,
                "id": str(outline.get("id") or uuid.uuid4().hex[:12]),
                "cell_type": self._normalize_cell_type(outline.get("cell_type", "markdown")),
                "metadata": self._dict_or_empty(outline.get("metadata")),
                "source": str(outline.get("source") or ""),
                "outputs": outputs,
                "output_text": "",
                "execution_count": outline.get("execution_count") if isinstance(outline.get("execution_count"), int) else None,
                "outline_only": False,
                "details_loaded": True,
                "source_line_count": int(outline.get("source_line_count") or 1),
                "has_output": bool(outline.get("has_output")),
                "output_count": int(outline.get("output_count") or (1 if outputs else 0)),
            }
        else:
            cell_obj = self._decode_json_value_safe(raw)
            if not isinstance(cell_obj, dict):
                raise SdkError("Invalid cell data")

            normalized = self._normalize_cells_payload([cell_obj])
            notebook_obj = {
                "cells": normalized,
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 5,
            }
            payload = self._serialize_notebook_for_ui(notebook_obj, notebook_path, 0, 1)
            cells = self._list_or_empty(payload.get("cells"))
            cell = cells[0] if cells else {}
            if isinstance(cell, dict):
                cell["index"] = idx
                cell["outline_only"] = False
                cell["details_loaded"] = True

        return {
            "cell_index": idx,
            "cell": cell,
            "total_cell_count": len(ranges),
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 5,
        }

    def _load_cell_media_payload_streaming(
        self,
        path: Path,
        notebook_path: str,
        cell_index: int,
        media_mime: str = "",
    ) -> dict[str, Any]:
        text = self._read_notebook_text(path)
        ranges = self._scan_cells_ranges_from_text(text)
        idx = self._coerce_int(cell_index, 0, minimum=0, maximum=max(0, len(ranges) - 1))
        if idx >= len(ranges):
            raise SdkError("Cell index out of range")

        start, end = ranges[idx]
        raw = text[start:end]
        cell_obj = self._decode_json_value_safe(raw)
        if not isinstance(cell_obj, dict):
            raise SdkError("Invalid cell payload")

        outputs = self._list_or_empty(cell_obj.get("outputs"))
        target_mime = str(media_mime or "").strip().lower()

        for out in outputs:
            if not isinstance(out, dict):
                continue

            meta_obj = self._dict_or_empty(out.get("metadata"))
            rel_path = str(meta_obj.get("neko_media_asset_path") or "").strip()
            if rel_path:
                resolved_path = self._resolve_media_asset_path(path, rel_path)
                if resolved_path and resolved_path.exists() and resolved_path.is_file():
                    mime_guess = str(meta_obj.get("neko_media_mime") or "").strip().lower() or self._guess_media_mime(resolved_path)
                    kind = str(meta_obj.get("neko_media_kind") or "").strip().lower()
                    if target_mime and mime_guess and target_mime != mime_guess:
                        pass
                    elif target_mime and (not mime_guess) and kind and target_mime.split("/")[0] != kind:
                        pass
                    else:
                        payload = self._read_media_asset_payload_for_notebook(path, notebook_path, rel_path)
                        if payload.get("base64"):
                            return {
                                "notebook_path": notebook_path,
                                "cell_index": idx,
                                "media_mime": str(payload.get("media_mime") or mime_guess or target_mime or "application/octet-stream"),
                                "size_bytes": int(payload.get("size_bytes") or 0),
                                "base64": str(payload.get("base64") or ""),
                            }

            data_obj = self._dict_or_empty(out.get("data"))
            candidate_keys = [str(k).lower() for k in data_obj.keys()]
            if target_mime and target_mime in candidate_keys:
                mime_key = target_mime
            else:
                mime_key = ""
                for key in candidate_keys:
                    if key.startswith("video/"):
                        mime_key = key
                        break
                if not mime_key:
                    continue

            value = data_obj.get(mime_key)
            if isinstance(value, list):
                base64_text = "".join(str(x) for x in value)
            else:
                base64_text = str(value or "")

            if not base64_text:
                continue

            return {
                "notebook_path": notebook_path,
                "cell_index": idx,
                "media_mime": mime_key,
                "size_bytes": max(0, (len(base64_text) * 3) // 4),
                "base64": base64_text,
            }

        raise SdkError("Media payload not found")

    def _read_notebook(self, path: Path) -> dict[str, Any]:
        with self._notebook_lock:
            if not path.exists():
                notebook = self._build_default_notebook(title=path.stem)
                self._write_notebook(path, notebook)
                return notebook

            text = path.read_text(encoding="utf-8", errors="replace")
            if nbformat is not None:
                node = nbformat.reads(text, as_version=4)
                notebook_obj = json.loads(nbformat.writes(node))
            else:
                notebook_obj = json.loads(text)

            return self._normalize_notebook(notebook_obj)

    def _write_notebook(self, path: Path, notebook: dict[str, Any]) -> None:
        normalized = self._normalize_notebook_for_storage(notebook)
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._notebook_lock:
            if nbformat is not None:
                node = nbformat.from_dict(normalized)
                nbformat.validate(node)
                text = nbformat.writes(node, version=4)
                path.write_text(text, encoding="utf-8")
            else:
                path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")

    def _normalize_notebook_for_storage(self, notebook: dict[str, Any]) -> dict[str, Any]:
        normalized = self._sanitize_json_value(self._normalize_notebook(notebook))
        storage_cells: list[dict[str, Any]] = []

        for item in self._list_or_empty(normalized.get("cells")):
            if not isinstance(item, dict):
                continue

            cell = self._sanitize_json_value(dict(item))
            metadata = self._dict_or_empty(cell.get("metadata"))
            ui_type = self._normalize_ui_cell_type_from_payload(cell.get("cell_type", ""), metadata)

            if ui_type == "view":
                # ipynb 标准不支持 view，落盘时转为 markdown 并保留类型标识。
                cell["cell_type"] = "markdown"
                metadata[_VIEW_CELL_METADATA_KEY] = "view"
                metadata["language"] = "markdown"
                cell.pop("outputs", None)
                cell.pop("execution_count", None)
            elif str(metadata.get(_VIEW_CELL_METADATA_KEY, "")).strip().lower() == "view":
                metadata.pop(_VIEW_CELL_METADATA_KEY, None)

            cell["metadata"] = metadata
            storage_cells.append(cell)

        normalized["cells"] = storage_cells
        return normalized

    def _normalize_notebook(self, raw: dict[str, Any]) -> dict[str, Any]:
        notebook = dict(raw) if isinstance(raw, dict) else {}
        cells_raw = self._list_or_empty(notebook.get("cells"))
        notebook["cells"] = self._normalize_cells_payload(cells_raw)

        metadata = self._dict_or_empty(notebook.get("metadata"))
        notebook["metadata"] = self._merge_notebook_metadata(metadata)
        notebook["nbformat"] = 4
        notebook["nbformat_minor"] = int(notebook.get("nbformat_minor", 5) or 5)
        return notebook

    def _normalize_cells_payload(self, cells_raw: list[Any]) -> list[dict[str, Any]]:
        cells: list[dict[str, Any]] = []
        for item in cells_raw:
            if not isinstance(item, dict):
                continue
            metadata = self._dict_or_empty(item.get("metadata"))
            cell_type = self._normalize_ui_cell_type_from_payload(item.get("cell_type", ""), metadata)
            cell_id = str(metadata.get("id", "")).strip() or uuid.uuid4().hex[:12]
            metadata["id"] = cell_id
            metadata["language"] = "python" if cell_type == "code" else "markdown"
            if cell_type == "view":
                metadata[_VIEW_CELL_METADATA_KEY] = "view"
            elif str(metadata.get(_VIEW_CELL_METADATA_KEY, "")).strip().lower() == "view":
                metadata.pop(_VIEW_CELL_METADATA_KEY, None)

            normalized_cell: dict[str, Any] = {
                "cell_type": cell_type,
                "metadata": metadata,
                "source": self._source_to_lines(self._source_to_text(item.get("source"))),
            }
            if cell_type == "code":
                outputs_raw = self._list_or_empty(item.get("outputs"))
                outputs: list[dict[str, Any]] = []
                for out in outputs_raw:
                    if isinstance(out, dict):
                        outputs.append(self._sanitize_json_value(dict(out)))
                normalized_cell["outputs"] = outputs
                execution_count = item.get("execution_count")
                normalized_cell["execution_count"] = execution_count if isinstance(execution_count, int) else None
            cells.append(normalized_cell)
        return cells

    def _merge_notebook_metadata(self, metadata: dict[str, Any]) -> dict[str, Any]:
        merged = dict(metadata or {})
        kernelspec = self._dict_or_empty(merged.get("kernelspec"))
        kernelspec.setdefault("display_name", "Python 3")
        kernelspec.setdefault("language", "python")
        kernelspec.setdefault("name", self._default_kernel_name)
        merged["kernelspec"] = kernelspec

        language_info = self._dict_or_empty(merged.get("language_info"))
        language_info.setdefault("name", "python")
        merged["language_info"] = language_info
        return merged

    def _serialize_notebook_for_ui(
        self,
        notebook: dict[str, Any],
        notebook_path: str,
        cell_offset: int = 0,
        cell_limit: int | None = None,
    ) -> dict[str, Any]:
        cells_raw = self._list_or_empty(notebook.get("cells"))
        total_cells = len(cells_raw)
        start = self._coerce_int(cell_offset, 0, minimum=0, maximum=max(0, total_cells))
        end = total_cells if cell_limit is None else min(total_cells, start + self._coerce_int(cell_limit, 0, minimum=0, maximum=2000))
        cells_slice = cells_raw[start:end]
        cells: list[dict[str, Any]] = []
        for index, cell in enumerate(cells_slice, start=start):
            if not isinstance(cell, dict):
                continue
            metadata = self._dict_or_empty(cell.get("metadata"))
            cell_id = str(metadata.get("id", "")).strip() or uuid.uuid4().hex[:12]
            metadata["id"] = cell_id
            cell_type = self._normalize_ui_cell_type_from_payload(cell.get("cell_type", ""), metadata)
            outputs_raw = self._list_or_empty(cell.get("outputs"))
            outputs: list[dict[str, Any]] = []
            for out in outputs_raw:
                if isinstance(out, dict):
                    outputs.append(self._sanitize_json_value(dict(out)))
            output_text = self._outputs_to_text(outputs)
            cells.append(
                {
                    "index": index,
                    "id": cell_id,
                    "cell_type": cell_type,
                    "metadata": metadata,
                    "source": self._source_to_text(cell.get("source")),
                    "outputs": outputs,
                    "output_text": output_text,
                    "execution_count": cell.get("execution_count") if isinstance(cell.get("execution_count"), int) else None,
                }
            )

        return {
            "path": notebook_path,
            "cells": cells,
            "total_cell_count": total_cells,
            "loaded_cell_offset": start,
            "loaded_cell_count": len(cells),
            "metadata": notebook.get("metadata") if isinstance(notebook.get("metadata"), dict) else {},
            "nbformat": int(notebook.get("nbformat", 4) or 4),
            "nbformat_minor": int(notebook.get("nbformat_minor", 5) or 5),
        }

    def _serialize_notebook_outline_for_ui(self, notebook: dict[str, Any], notebook_path: str) -> dict[str, Any]:
        cells_raw = self._list_or_empty(notebook.get("cells"))
        cells: list[dict[str, Any]] = []

        for index, cell in enumerate(cells_raw):
            if not isinstance(cell, dict):
                continue

            metadata = self._dict_or_empty(cell.get("metadata"))
            cell_id = str(metadata.get("id", "")).strip() or uuid.uuid4().hex[:12]
            metadata["id"] = cell_id
            cell_type = self._normalize_ui_cell_type_from_payload(cell.get("cell_type", ""), metadata)

            source_text = self._source_to_text(cell.get("source"))
            source_lines = max(1, len(source_text.splitlines()) if source_text else 1)

            outputs_raw = self._list_or_empty(cell.get("outputs"))
            has_output = bool(outputs_raw)

            cells.append(
                {
                    "index": index,
                    "id": cell_id,
                    "cell_type": cell_type,
                    "metadata": metadata,
                    "source": "",
                    "outputs": [],
                    "output_text": "",
                    "execution_count": cell.get("execution_count") if isinstance(cell.get("execution_count"), int) else None,
                    "outline_only": True,
                    "details_loaded": False,
                    "source_line_count": source_lines,
                    "has_output": has_output,
                    "output_count": len(outputs_raw),
                }
            )

        return {
            "path": notebook_path,
            "cells": cells,
            "total_cell_count": len(cells),
            "loaded_cell_offset": 0,
            "loaded_cell_count": len(cells),
            "metadata": notebook.get("metadata") if isinstance(notebook.get("metadata"), dict) else {},
            "nbformat": int(notebook.get("nbformat", 4) or 4),
            "nbformat_minor": int(notebook.get("nbformat_minor", 5) or 5),
            "outline_only": True,
        }

    def _outputs_to_text(self, outputs: list[dict[str, Any]]) -> str:
        chunks: list[str] = []
        for out in outputs:
            if not isinstance(out, dict):
                continue
            output_type = str(out.get("output_type", ""))
            if output_type == "stream":
                chunks.append(self._sanitize_text(out.get("text", "")))
                continue
            if output_type in {"execute_result", "display_data"}:
                data_obj = self._dict_or_empty(self._sanitize_json_value(out.get("data")))
                meta_obj = self._dict_or_empty(self._sanitize_json_value(out.get("metadata")))
                asset_path = self._sanitize_text(meta_obj.get("neko_media_asset_path", "")).strip()
                asset_kind = self._sanitize_text(meta_obj.get("neko_media_kind", "")).strip().lower()
                if asset_path and asset_kind in {"image", "video"}:
                    chunks.append(f"[媒体文件输出: {asset_kind} -> {asset_path}]")
                    continue

                text_plain = data_obj.get("text/plain")
                if isinstance(text_plain, list):
                    chunks.append("".join(self._sanitize_text(x) for x in text_plain))
                elif text_plain is not None:
                    chunks.append(self._sanitize_text(text_plain))
                else:
                    keys = [str(k) for k in data_obj.keys()]
                    if any(k.startswith("image/") or k.startswith("video/") for k in keys):
                        media_tags = [k for k in keys if k.startswith("image/") or k.startswith("video/")]
                        chunks.append(f"[媒体输出: {', '.join(media_tags)}]")
                    elif "application/vnd.plotly.v1+json" in data_obj:
                        chunks.append("[Plotly 图表输出]")
                    elif "text/html" in data_obj:
                        chunks.append("[HTML 输出]")
                    else:
                        chunks.append(self._sanitize_text(json.dumps(data_obj, ensure_ascii=False)))
                continue
            if output_type == "error":
                traceback_obj = out.get("traceback") if isinstance(out.get("traceback"), list) else []
                if traceback_obj:
                    chunks.append("\n".join(self._sanitize_text(x) for x in traceback_obj))
                else:
                    ename = self._sanitize_text(out.get("ename", "Error"))
                    evalue = self._sanitize_text(out.get("evalue", ""))
                    chunks.append(f"{ename}: {evalue}")
                continue

        text = "\n".join(chunk.rstrip("\n") for chunk in chunks if str(chunk).strip())
        return text.strip()

    def _outputs_contain_visual(self, outputs: list[dict[str, Any]]) -> bool:
        visual_mimes = {
            "image/png",
            "image/jpeg",
            "image/svg+xml",
            "application/vnd.plotly.v1+json",
        }
        for out in outputs:
            if not isinstance(out, dict):
                continue
            output_type = str(out.get("output_type", ""))
            if output_type not in {"execute_result", "display_data"}:
                continue
            metadata_obj = self._dict_or_empty(out.get("metadata"))
            if str(metadata_obj.get("neko_media_kind", "")).strip().lower() in {"image", "video"}:
                return True
            data_obj = self._dict_or_empty(out.get("data"))
            for key in data_obj.keys():
                mime_key = str(key)
                if mime_key in visual_mimes or mime_key.startswith("image/") or mime_key.startswith("video/"):
                    return True
        return False

    def _read_clipboard_file_paths(self) -> list[str]:
        if os.name != "nt":
            return []
        return self._read_windows_clipboard_file_paths()

    def _read_windows_clipboard_file_paths(self) -> list[str]:
        try:
            import ctypes
            from ctypes import wintypes

            CF_HDROP = 15
            user32 = ctypes.windll.user32
            shell32 = ctypes.windll.shell32

            user32.OpenClipboard.argtypes = [wintypes.HWND]
            user32.OpenClipboard.restype = wintypes.BOOL
            user32.CloseClipboard.argtypes = []
            user32.CloseClipboard.restype = wintypes.BOOL
            user32.IsClipboardFormatAvailable.argtypes = [wintypes.UINT]
            user32.IsClipboardFormatAvailable.restype = wintypes.BOOL
            user32.GetClipboardData.argtypes = [wintypes.UINT]
            user32.GetClipboardData.restype = wintypes.HANDLE

            shell32.DragQueryFileW.argtypes = [wintypes.HANDLE, wintypes.UINT, wintypes.LPWSTR, wintypes.UINT]
            shell32.DragQueryFileW.restype = wintypes.UINT

            if not user32.OpenClipboard(None):
                return []

            paths: list[str] = []
            try:
                if not user32.IsClipboardFormatAvailable(CF_HDROP):
                    return []

                handle = user32.GetClipboardData(CF_HDROP)
                if not handle:
                    return []

                count = int(shell32.DragQueryFileW(handle, 0xFFFFFFFF, None, 0))
                if count <= 0:
                    return []

                for idx in range(count):
                    length = int(shell32.DragQueryFileW(handle, idx, None, 0))
                    if length <= 0:
                        continue
                    buf = ctypes.create_unicode_buffer(length + 1)
                    shell32.DragQueryFileW(handle, idx, buf, length + 1)
                    value = str(buf.value or "").strip()
                    if value:
                        paths.append(value)
            finally:
                user32.CloseClipboard()

            return paths
        except Exception:
            return []

    @staticmethod
    def _guess_media_mime(path: Path) -> str:
        guessed, _encoding = mimetypes.guess_type(str(path))
        mime = str(guessed or "").strip().lower()
        if not (mime.startswith("image/") or mime.startswith("video/")):
            suffix = path.suffix.lower()
            if suffix in {".jpg", ".jpeg"}:
                mime = "image/jpeg"
            elif suffix in {".png"}:
                mime = "image/png"
            elif suffix in {".gif"}:
                mime = "image/gif"
            elif suffix in {".webp"}:
                mime = "image/webp"
            elif suffix in {".bmp"}:
                mime = "image/bmp"
            elif suffix in {".svg"}:
                mime = "image/svg+xml"
            elif suffix in {".mp4"}:
                mime = "video/mp4"
            elif suffix in {".webm"}:
                mime = "video/webm"
            elif suffix in {".mov"}:
                mime = "video/quicktime"
            elif suffix in {".mkv"}:
                mime = "video/x-matroska"
            elif suffix in {".avi"}:
                mime = "video/x-msvideo"
            elif suffix in {".flv"}:
                mime = "video/x-flv"
            elif suffix in {".wmv"}:
                mime = "video/x-ms-wmv"
            else:
                mime = ""

        if mime.startswith("image/") or mime.startswith("video/"):
            return mime
        return ""

    def _media_kind_from_mime(self, mime: str) -> str:
        value = str(mime or "").strip().lower()
        if value.startswith("video/"):
            return "video"
        if value.startswith("image/"):
            return "image"
        return ""

    @staticmethod
    def _sanitize_filename_for_fs(name: str) -> str:
        value = str(name or "").strip().replace("\\", "/")
        value = value.split("/")[-1].strip()
        if not value:
            return "asset"
        value = re.sub(r'[\x00-\x1f<>:"/\\|?*]', "_", value)
        value = value.rstrip(". ")
        return value or "asset"

    def _build_asset_file_name(self, original_name: str, default_suffix: str) -> str:
        safe_original = self._sanitize_filename_for_fs(original_name)
        base = Path(safe_original).stem.strip()
        suffix = Path(safe_original).suffix.strip()
        if not suffix:
            suffix = str(default_suffix or "").strip() or ""
        if not base:
            base = "asset"
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{base}_{stamp}{suffix}"

    @classmethod
    def _normalize_ui_cell_type_from_payload(
        cls,
        cell_type: Any,
        metadata: dict[str, Any] | None = None,
        default: str = "markdown",
    ) -> str:
        normalized = cls._normalize_cell_type(cell_type, default=default)
        meta = metadata if isinstance(metadata, dict) else {}
        marker = str(meta.get(_VIEW_CELL_METADATA_KEY, "")).strip().lower()
        if normalized == "markdown" and marker == "view":
            return "view"
        return normalized

    @staticmethod
    def _normalize_cell_type(cell_type: Any, default: str = "markdown") -> str:
        value = str(cell_type or "").strip().lower()
        if value == "code":
            return "code"
        if value == "view":
            return "view"
        if value == "markdown":
            return "markdown"

        fallback = str(default or "markdown").strip().lower()
        if fallback in _ALLOWED_NOTEBOOK_CELL_TYPES:
            return fallback
        return "markdown"

    @staticmethod
    def _normalize_media_asset_rel_path(asset_rel_path: str) -> str | None:
        rel = str(asset_rel_path or "").strip().replace("\\", "/")
        if not rel:
            return None
        if rel.startswith("./"):
            rel = rel[2:]
        if rel.startswith("/"):
            rel = rel[1:]

        # Strictly limit media paths to a single assets root with fixed subdirs.
        if not re.match(r"^assets/(image|video)/[^/]+$", rel, flags=re.IGNORECASE):
            return None
        return rel

    def _resolve_media_asset_path(self, notebook_path: Path, asset_rel_path: str) -> Path | None:
        workspace_root = self._workspace_root.resolve()
        rel = self._normalize_media_asset_rel_path(asset_rel_path)
        if rel is None:
            return None
        candidate = (workspace_root / rel).resolve()
        try:
            candidate.relative_to(workspace_root)
        except Exception:
            return None

        if candidate.exists():
            return candidate

        # Keep compatibility for legacy notebooks that still reference notebook-local assets.
        legacy_base = notebook_path.parent.resolve()
        legacy_candidate = (legacy_base / rel).resolve()
        try:
            legacy_candidate.relative_to(legacy_base)
        except Exception:
            return None
        if legacy_candidate.exists():
            return legacy_candidate
        return candidate

    def _store_media_asset_for_notebook(
        self,
        notebook_abs_path: Path,
        notebook_rel_path: str,
        media_mime: str,
        base64_text: str,
        original_name: str,
    ) -> dict[str, Any]:
        mime = str(media_mime or "").strip().lower()
        kind = self._media_kind_from_mime(mime)
        if kind not in {"video", "image"}:
            raise SdkError("Unsupported media mime")

        raw_base64 = str(base64_text or "").strip()
        if not raw_base64:
            raise SdkError("Empty media payload")

        try:
            raw = base64.b64decode(raw_base64, validate=True)
        except (binascii.Error, ValueError):
            try:
                raw = base64.b64decode(raw_base64)
            except Exception as exc:
                raise SdkError(f"Invalid media base64: {exc}") from exc

        size = len(raw)
        if size <= 0:
            raise SdkError("Empty media bytes")

        return self._store_media_bytes_for_notebook(
            notebook_abs_path=notebook_abs_path,
            notebook_rel_path=notebook_rel_path,
            media_mime=mime,
            media_kind=kind,
            raw=raw,
            original_name=original_name,
        )

    def _store_media_asset_for_notebook_from_path(
        self,
        notebook_abs_path: Path,
        notebook_rel_path: str,
        source_path: str,
        media_mime: str = "",
        original_name: str = "",
    ) -> dict[str, Any]:
        src = Path(str(source_path or "")).expanduser()
        if (not src.exists()) or (not src.is_file()):
            raise SdkError("Source media file not found")

        mime = str(media_mime or "").strip().lower() or self._guess_media_mime(src)
        kind = self._media_kind_from_mime(mime)
        if kind not in {"video", "image"}:
            raise SdkError("Unsupported media mime")

        try:
            raw = src.read_bytes()
        except Exception as exc:
            raise SdkError(f"Failed to read source media file: {exc}") from exc

        if not original_name:
            original_name = src.name

        return self._store_media_bytes_for_notebook(
            notebook_abs_path=notebook_abs_path,
            notebook_rel_path=notebook_rel_path,
            media_mime=mime,
            media_kind=kind,
            raw=raw,
            original_name=original_name,
        )

    def _store_media_bytes_for_notebook(
        self,
        notebook_abs_path: Path,
        notebook_rel_path: str,
        media_mime: str,
        media_kind: str,
        raw: bytes,
        original_name: str,
    ) -> dict[str, Any]:
        mime = str(media_mime or "").strip().lower()
        kind = str(media_kind or "").strip().lower()
        if kind not in {"video", "image"}:
            raise SdkError("Unsupported media kind")
        if not raw:
            raise SdkError("Empty media bytes")

        suffix = mimetypes.guess_extension(mime) or ""
        if mime == "image/svg+xml":
            suffix = ".svg"
        file_name = self._build_asset_file_name(original_name, suffix)

        workspace_root = self._workspace_root.resolve()
        asset_dir = workspace_root / "assets" / ("video" if kind == "video" else "image")
        asset_dir.mkdir(parents=True, exist_ok=True)

        candidate = asset_dir / file_name
        dedupe = 1
        while candidate.exists():
            stem = candidate.stem
            ext = candidate.suffix
            candidate = asset_dir / f"{stem}_{dedupe}{ext}"
            dedupe += 1

        with self._notebook_lock:
            candidate.write_bytes(raw)

        rel_path = candidate.relative_to(workspace_root).as_posix()
        src_attr = f"./{rel_path}"
        size_bytes = len(raw)
        if kind == "video":
            html = f"<video controls preload=\"metadata\" src=\"{src_attr}\"></video>"
        else:
            html = f"<img src=\"{src_attr}\" alt=\"{self._sanitize_text(Path(original_name or file_name).name)}\" />"

        return {
            "notebook_path": notebook_rel_path,
            "asset_kind": kind,
            "asset_rel_path": rel_path,
            "asset_src": src_attr,
            "media_mime": mime,
            "size_bytes": size_bytes,
            "original_name": self._sanitize_text(Path(original_name or file_name).name),
            "saved_name": candidate.name,
            "html": html,
        }

    def _read_media_asset_payload_for_notebook(
        self,
        notebook_abs_path: Path,
        notebook_rel_path: str,
        asset_rel_path: str,
    ) -> dict[str, Any]:
        resolved = self._resolve_media_asset_path(notebook_abs_path, asset_rel_path)
        if resolved is None or (not resolved.exists()) or (not resolved.is_file()):
            raise SdkError("Media asset not found")

        raw = resolved.read_bytes()
        mime = self._guess_media_mime(resolved)
        if not mime:
            guessed, _ = mimetypes.guess_type(str(resolved))
            mime = str(guessed or "application/octet-stream")

        workspace_root = self._workspace_root.resolve()
        try:
            rel_path = resolved.relative_to(workspace_root).as_posix()
        except Exception:
            rel_path = resolved.relative_to(notebook_abs_path.parent).as_posix()
        return {
            "notebook_path": notebook_rel_path,
            "asset_rel_path": rel_path,
            "media_mime": mime,
            "size_bytes": len(raw),
            "base64": base64.b64encode(raw).decode("ascii"),
        }

    def _collect_cell_asset_paths(self, cell: dict[str, Any], notebook_abs_path: Path) -> set[Path]:
        paths: set[Path] = set()
        outputs = self._list_or_empty(cell.get("outputs"))
        for out in outputs:
            if not isinstance(out, dict):
                continue
            meta = self._dict_or_empty(out.get("metadata"))
            rel = str(meta.get("neko_media_asset_path") or "").strip()
            if not rel:
                continue
            resolved = self._resolve_media_asset_path(notebook_abs_path, rel)
            if resolved is not None:
                paths.add(resolved)
        return paths

    def _collect_notebook_asset_paths(self, notebook: dict[str, Any], notebook_abs_path: Path) -> set[Path]:
        paths: set[Path] = set()
        for cell in self._cells_from_notebook(notebook):
            paths |= self._collect_cell_asset_paths(cell, notebook_abs_path)
        return paths

    @staticmethod
    def _delete_asset_files(paths: set[Path]) -> None:
        for path in paths:
            try:
                if path.exists() and path.is_file():
                    path.unlink()
            except Exception:
                continue

    @staticmethod
    def _extract_markdown_title(content: str) -> str:
        for line in str(content or "").splitlines():
            text = line.strip()
            if not text:
                continue
            if text.startswith("#"):
                return text.lstrip("#").strip() or "Untitled"
            return text[:80]
        return "Untitled"

    @staticmethod
    def _dict_or_empty(value: Any) -> dict[str, Any]:
        return dict(value) if isinstance(value, dict) else {}

    @staticmethod
    def _list_or_empty(value: Any) -> list[Any]:
        return list(value) if isinstance(value, list) else []

    @staticmethod
    def _source_to_text(source: Any) -> str:
        if isinstance(source, list):
            return JupyterNotebookPlugin._sanitize_text("".join(str(x) for x in source))
        if source is None:
            return ""
        return JupyterNotebookPlugin._sanitize_text(source)

    @staticmethod
    def _source_to_lines(source: str) -> list[str]:
        text = JupyterNotebookPlugin._sanitize_text(source)
        if text == "":
            return [""]
        lines = text.splitlines(keepends=True)
        if not lines:
            return [text]
        return lines

    @staticmethod
    def _clip_text(text: str, max_chars: int) -> str:
        value = JupyterNotebookPlugin._sanitize_text(text)
        if len(value) <= max_chars:
            return value
        return f"{value[:max_chars]}..."

    @staticmethod
    def _sanitize_text(value: Any) -> str:
        text = "" if value is None else str(value)
        if not text:
            return ""
        cleaned_chars: list[str] = []
        for ch in text:
            code = ord(ch)
            if 0xD800 <= code <= 0xDFFF:
                cleaned_chars.append("\ufffd")
            elif code == 0:
                cleaned_chars.append("")
            else:
                cleaned_chars.append(ch)
        return "".join(cleaned_chars)

    @staticmethod
    def _sanitize_json_value(value: Any) -> Any:
        if isinstance(value, dict):
            cleaned: dict[str, Any] = {}
            for key, item in value.items():
                safe_key = JupyterNotebookPlugin._sanitize_text(key)
                cleaned[str(safe_key)] = JupyterNotebookPlugin._sanitize_json_value(item)
            return cleaned
        if isinstance(value, list):
            return [JupyterNotebookPlugin._sanitize_json_value(item) for item in value]
        if isinstance(value, tuple):
            return [JupyterNotebookPlugin._sanitize_json_value(item) for item in value]
        if isinstance(value, str):
            return JupyterNotebookPlugin._sanitize_text(value)
        return value

    @staticmethod
    def _coerce_bool(value: Any, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
        return bool(default)

    @staticmethod
    def _coerce_int(value: Any, default: int, minimum: int | None = None, maximum: int | None = None) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = int(default)
        if minimum is not None:
            parsed = max(minimum, parsed)
        if maximum is not None:
            parsed = min(maximum, parsed)
        return parsed

    @staticmethod
    def _coerce_float(value: Any, default: float, minimum: float | None = None) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            parsed = float(default)
        if minimum is not None:
            parsed = max(minimum, parsed)
        return parsed
