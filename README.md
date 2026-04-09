# Jupyter Notebook 插件

## 功能概览

- Notebook 文件管理：创建、加载、保存、列出
- 单元格管理：新增、删除、修改 code/markdown
- 代码执行：使用 `jupyter_client` + `ipykernel` 执行代码单元
- 内核容错降级：若 Kernel 启动失败，自动切换到插件隔离 Python 子进程执行，避免整条链路报错中断
- 输出回填：保存 `outputs` 与 `execution_count`，并在 UI 直接展示
- 已强制禁用向主对话模型的后台事件推送，避免执行日志与 ID 串污染对话
- 插件内隔离运行环境：插件启动时构建插件私有 `venv`
- 依赖自举安装：自动检查并安装 `jupyter_client`、`ipykernel`、`nbformat`、`numpy`、`matplotlib`、`seaborn`、`pandas`、`plotly` 等依赖
- 绘图渲染策略：执行前自动注入 inline 后端与 Plotly MIME 渲染，优先保证图表内嵌在输出区展示
- 插件 Web UI：`http://127.0.0.1:48916/plugin/jupyter_notebook/ui/`

## 推送事件类型

- `notebook_created`
- `notebook_saved`
- `cell_added`
- `cell_deleted`
- `markdown_cell_updated`
- `code_cell_updated`
- `cell_execution_status`（queued/running/succeeded/failed）
- `cell_execution_stream`（仅插件内部事件队列）
- `cell_execution_output`（仅插件内部事件队列）
- `kernel_restarted`
- `kernel_shutdown`

每条事件元数据默认包含：

- `timestamp`
- `event_type`
- `notebook_path`
- `cell_id`（如有）
- `cell_type`（如有）

## 目录结构

```text
jupyter_notebook/
├── __init__.py
├── plugin.toml
├── README.md
└── static/
    ├── index.html
    ├── main.js
    └── style.css
```

## 配置项（plugin.toml）

```toml
[jupyter_notebook]
kernel_name = "python3"
execution_timeout_sec = 90
bootstrap_timeout_sec = 420
auto_setup_env = true
auto_install_deps = true
push_priority = 6
push_content_updates = false
push_to_chat_model = false
max_event_queue = 1000
```

## 运行依赖

- 插件会优先在 `data/runtime/venv/` 使用隔离环境
- 若插件进程检测到依赖缺失，会在启动时自动安装
- 若网络不可达导致安装失败，编辑/保存仍可用，执行入口会返回运行环境错误

## UI 调用方式

前端通过插件服务器 `POST /runs` 调用以下入口：

- `get_ui_info`
- `list_notebooks`
- `create_notebook`
- `load_notebook`
- `save_notebook`
- `add_cell`
- `delete_cell`
- `update_cell`
- `execute_cell`
- `restart_kernel`
- `shutdown_kernel`
- `list_recent_events`

## 说明

- 本插件只在插件目录内工作，不修改主项目代码。
- Notebook 文件存储于插件数据目录：`<plugin_dir>/data/workspace/`。
- 隔离运行环境目录：`<plugin_dir>/data/runtime/venv/`。
