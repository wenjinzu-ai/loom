# Loom — 用 Rust 编织的 Agent OS

> **Loom**（织机）：将无数条 Agent 执行线程、工具能力、记忆与上下文，编织成一张协同解决复杂任务的智能织物。

Loom 是一个用 Rust 构建的 **Agent Operating System**——一个为自主智能体（Agent）设计的运行时与编排平台。它不是单一的聊天机器人，而是一个能同时管理成千上万个 Agent 生命周期、隔离、记忆、上下文与协作的"操作系统"。

---

## 为什么叫 Loom？

**织机（Loom）** 的隐喻贯穿整个系统设计：

- **经线（Warp）** = 长生命周期的 Agent 实例与会话线程，贯穿整个任务执行过程
- **纬线（Weft）** = 工具调用、记忆读写、上下文压缩、子 Agent 委派等横向操作
- **织物（Fabric）** = 多 Agent 协作产生的最终成果——复杂任务的完整解决方案

正如织机通过精确的经线张力与纬线穿梭，将分散的丝线编织成坚韧的布匹，Loom 通过：

- **分级隔离后端**（协程 → 进程 → 容器 → WASM）为每条"经线"提供安全边界
- **上下文压缩引擎** 为"纬线"穿梭留出空间，避免上下文窗口撑爆
- **委派与并发** 让多 Agent 像多把梭子一样并行工作
- **Checkpoint 持久化** 让织物在中断后能从断点继续编织

---

## 为什么用 Rust？

Agent OS 是基础设施级软件，要求极高的可靠性、并发性能与内存安全。Rust 恰好提供了这些特性：

### 内存安全，零 GC 开销
- 所有权（Ownership）+ 借用检查（Borrow Checker）在编译期消除数据竞争、悬垂指针、缓冲区溢出
- 无垃圾回收，Agent 循环延迟可预测，无 GC 停顿导致的超时

### 并发安全由编译器保证
- `Send` / `Sync` trait 让类型系统自动验证跨线程共享的安全性
- tokio 异步运行时支持数千个 Agent 并发运行，每个 Agent 是一个独立 task
- `parking_lot` 提供高性能锁，`tokio::sync` 提供异步原语

### 零成本抽象
- 泛型、trait 对象、宏在编译期展开，运行时无额外开销
- 各隔离后端（Coroutine / Process / Container / Wasm）通过统一 trait 抽象，选择不同后端零性能损失

### 类型系统即文档
- 强类型 + `serde` 派生，配置文件、checkpoint、消息协议在编译期校验
- `thiserror` + `anyhow` 提供精确的错误分类（`LlmErrorKind`、`ToolFailureKind`、`AgentErrorKind`）

### 生态成熟
- `axum` 提供高性能 HTTP/SSE 服务
- `sqlx` 编译期校验 SQL，支持 PostgreSQL 持久化
- `reqwest` + `rustls` 提供无 OpenSSL 依赖的 HTTP 客户端

---

## Agent OS 的核心优势

### 1. 分级隔离（Isolation）

| 级别 | 实现 | 适用场景 |
|------|------|----------|
| **Coroutine** | tokio task，同进程 | 可信 Agent，零开销 |
| **Process** | 子进程 + IPC | 内存隔离 |
| **Container** | Docker / containerd | 文件系统 + 网络隔离 |
| **Wasm** | wasmtime | 不可信代码，最强沙箱 |

按需升级隔离级别，既保证性能又保障安全。子 Agent 可继承父 Agent 的租户/用户作用域（`MemoryScope`）。

### 2. 智能上下文管理（Context Engine）

- **动态阈值**：基于模型 `context_length` 自动计算压缩触发点，避免硬编码
- **分层压缩**：先低成本工具修剪（截断、去重），再 LLM 结构化摘要，减少昂贵的 LLM 调用
- **压缩状态持久化**：摘要链随 checkpoint 保存，resume 后逻辑连贯
- **Token 追踪**：通过消息数变化检测过期 token 估值，防止压缩判断滞后

### 3. 多 Agent 委派（Delegation）

- `spawn_agent` 支持单目标 `goal` 或批量 `tasks` 数组
- 顶层 Agent 可 `background=true` 异步启动子 Agent，结果通过推模式回注会话
- 子 Agent 始终同步阻塞返回结果
- 并发限制（`max_concurrent_children`）+ 信号量控制资源
- 后台任务注册表 + 陈旧检测守护进程 + 崩溃恢复持久化

### 4. Checkpoint 与恢复

- 每轮（或每 N 轮）保存 step checkpoint，`interrupt` 时强制保存
- 支持从任意 checkpoint 恢复（`resume_from_checkpoint`）
- 多租户物理隔离：`namespace=checkpoint:{tenant}:{user}`
- 压缩状态、委派深度、工具集、父 Agent ID 全量恢复

### 5. 记忆子系统（Memory）

三层抽象：
1. **MemoryStore**：显式记忆（curated memory），双目标 `user` / `memory`，字符预算，批量原子操作
2. **MemoryProvider**：生命周期钩子（prefetch / sync_turn / on_pre_compress / on_session_end），支持 mem0 / honcho 等外部后端
3. **MemoryManager**：扇出所有钩子，provider 间故障隔离，后台串行执行

附带威胁扫描（prompt injection / exfiltration 检测）、`<memory-context>` 围栏、trivial prompt 过滤。

### 6. 工具防护（Guardrails）

- 同一 `(tool_name, args_hash)` 滑动窗口去重
- 连续失败次数上限 → guardrail halt
- 循环检测（滑动窗口内重复模式）
- 单轮 `web_search` / `spawn_agent` 调用上限

### 7. 人在回路（HITL）

- `interrupt` 工具暂停 Agent 执行并保存 checkpoint
- `resume` / `resume_from_checkpoint` 恢复执行
- 支持外部注入 interrupt value

---

## 架构概览

```
┌─────────────────────────────────────────────────────────┐
│                    loom-chat (HTTP/SSE)                  │
│   POST /chat  ·  /agents  ·  /sessions  ·  /capabilities│
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                    loom-agent (大脑)                     │
│  AgentLoop · ContextCompressor · DelegationManager      │
│  Checkpoint · Guardrails · HITL · ApiRetry              │
└──────┬──────────────┬──────────────┬────────────────────┘
       │              │              │
┌──────▼─────┐ ┌──────▼─────┐ ┌──────▼─────┐
│loom-llm    │ │loom-tools  │ │loom-memory │
│OpenAI 兼容 │ │内置工具集   │ │记忆子系统   │
└────────────┘ └────────────┘ └────────────┘
       │              │              │
┌──────▼──────────────▼──────────────▼────────────────────┐
│                    loom-isolation                        │
│      Coroutine · Process · Container · Wasm             │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                    loom-infra / loom-core                │
│   PostgreSQL · InMemory · Registry · Bus · Lifecycle     │
└─────────────────────────────────────────────────────────┘
```

### Crate 职责

| Crate | 职责 |
|-------|------|
| `loom-core` | 核心 trait 与类型（Capability、Lifecycle、Isolation、Checkpoint、Message） |
| `loom-llm` | LLM Provider 抽象（OpenAI 兼容协议） |
| `loom-agent` | Agent 对话循环（大脑）：决策→工具→回填→循环 |
| `loom-tools` | 内置工具集（文件、代码、Web、看板、记忆、技能等） |
| `loom-memory` | 记忆子系统（curated memory + 外部 provider） |
| `loom-isolation` | 分级隔离后端（协程/进程/容器/WASM） |
| `loom-adapters` | 协议适配（native / MCP / A2A） |
| `loom-infra` | 基础设施（PostgreSQL / 内存存储、注册表、总线） |
| `loom-chat` | HTTP/SSE 服务入口 |

---

## 快速开始

### 1. 配置

```bash
cp config.example.toml config.toml
# 编辑 config.toml，填入 LLM base_url / api_key / model
```

### 2. 数据库（可选，用于 checkpoint 持久化）

```bash
psql -U postgres -c "CREATE DATABASE loom;"
psql -U postgres -d loom -f migrations/001_full_schema.sql
```

### 3. 运行

```bash
cargo run -p loom-chat
```

服务启动在 `http://127.0.0.1:3000`。

### 4. 对话

```bash
curl -N -X POST http://127.0.0.1:3000/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "你好，请介绍一下你自己", "tenant_id": "default", "user_id": "user1"}'
```

---

## API 概览

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/chat` | 流式对话（SSE） |
| GET | `/agents` | 列出运行中 Agent |
| POST | `/agents` | 启动 Agent |
| GET | `/agents/:id` | 查询 Agent 状态 |
| DELETE | `/agents/:id` | 销毁 Agent |
| POST | `/agents/:id/messages` | 向 Agent 发消息 |
| GET | `/capabilities` | 列出已注册能力 |
| GET | `/sessions` | 列出会话 |
| GET | `/sessions/:id` | 查询会话 |
| POST | `/sessions/:id/resume` | 恢复会话 |
| POST | `/sessions/:id/resume-from-checkpoint` | 从 checkpoint 恢复 |

---

## 内置工具集

| 工具集 | 说明 |
|--------|------|
| `filesystem` | 文件读写、目录操作 |
| `code` | 代码搜索、编辑、执行 |
| `web` | Web 搜索、抓取 |
| `todo` | 任务清单管理 |
| `kanban` | 看板项目管理 |
| `memory` | 记忆读写 |
| `skills` | 技能注册与调用 |
| `clarify` | 向用户提问澄清 |
| `cron` | 定时任务 |
| `spawn_agent` | 委派子 Agent |
| `interrupt` | 暂停等待人工输入 |

---

## 配置说明

详见 `config.example.toml`，主要配置块：

- `[server]` — 监听地址
- `[database]` — PostgreSQL 连接（checkpoint 持久化）
- `[llm]` — LLM Provider（base_url、api_key、model、上下文窗口）
- `[agent]` — Agent 循环（迭代次数、委派深度、并发数）
- `[agent.prompt]` — 结构化提示词（stable/context/volatile 三层）
- `[agent.compression]` — 上下文压缩
- `[agent.api_retry]` — API 重试（指数退避）
- `[agent.guardrails]` — 工具防护
- `[agent.liveness]` — 活性看门狗

---

## License

MIT