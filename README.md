# Loom — 用 Rust 编织的 Agent OS

> **Loom**（织机）：将无数条 Agent 执行线程、工具能力、记忆与上下文，编织成一张协同解决复杂任务的智能织物。

Loom 是一个用 Rust 构建的 **企业级 Agent Operating System**——为自主智能体（Agent）设计的运行时与编排平台，能够同时管理成千上万个 Agent 的生命周期、隔离、记忆、上下文与协作。

> **核心理念**：业务方只需专注于开发 **Tools / MCP / A2A** 能力，即可无缝接入 Loom 平台。编排、隔离、记忆、上下文压缩、Checkpoint、多 Agent 协作等基础设施能力由平台统一提供，业务无需重复造轮子。

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

Agent OS 是基础设施级软件，运行在服务器上长期调度成千上万个 Agent。Rust 的核心特性直接服务于 Agent 编排，而非单纯的语言炫技。与 Python 对比，Rust 在服务器场景下的优势尤为明显：

### Vibe Coding：磨平语言门槛，不怕突破

Rust 的学习曲线曾是最大障碍，但在 AI 编程（vibe coding）时代被大幅消解：编译器就是最严格的结对程序员，所有权/借用/生命周期错误在编译期直接给出，AI 辅助下快速修复；描述意图让 AI 生成代码，编译器验证正确性，形成「意图 → 生成 → 编译 → 修复」的正循环。Rust 不再是"高手的玩具"，而是"AI 时代安全编写系统软件的默认选择"——不要怕突破，编译器替你兜底。

### 调度：确定性编排，无 GC 停顿

Agent 编排要求**每个循环迭代的延迟可预测**。Rust 无垃圾回收，不会因 GC 触发导致 Agent 超时或请求堆积；Python 的 GIL 与 GC 会引入不可预测的停顿，高并发下尾延迟明显放大。tokio 异步运行时采用 work-stealing 调度，数千个 Agent 作为独立 task 在多线程间动态负载均衡；Agent 生命周期（创建/暂停/恢复/销毁）由 `LifecycleManager` 精确管理，无幽灵任务。

### 性能：零抽象开销，纯调度成本可忽略

Agent 的瓶颈在 LLM 调用，调度层不能成为额外瓶颈。Rust 泛型、trait 对象、宏在编译期展开，各隔离后端（Coroutine / Process / Container / Wasm）通过统一 trait 抽象且切换零损失，LLVM 优化带来接近 C/C++ 的执行效率——LLM 调用之外的纯调度开销可忽略。Python 解释器开销大，多协程并发时上下文切换与事件循环本身就会消耗可观 CPU。

### 资源：内存安全 + 精确控制

- 所有权 + 借用检查在编译期消除数据竞争、悬垂指针，无需运行时检查
- 常驻内存比 Python 低一个数量级（无对象头、无引用计数、无 GC 元数据），单节点承载更多 Agent
- `Send` / `Sync` trait 自动验证跨线程共享安全，杜绝并发 bug
- 服务器上长驻运行时，内存泄露与资源未释放问题在 Rust 中更易被编译器捕获

---

## Agent OS 的核心优势

### 1. 分级隔离（Isolation）

| 级别 | 实现 | 适用场景 |
|------|------|----------|
| **Coroutine** | tokio task，同进程 | 可信 Agent，零开销 |
| **Process** | 子进程 + IPC | 内存隔离 |
| **Container** | Docker / containerd | 文件系统 + 网络隔离 |
| **Wasm** | wasmtime | 不可信代码，最强沙箱 |

按需升级隔离级别，既保证性能又保障安全。

### 2. 多 Agent 编排与委派（Orchestration）

- `spawn_agent` 支持单目标 `goal` 或批量 `tasks` 数组，自动并发执行
- 顶层 Agent 可 `background=true` 异步启动子 Agent，结果通过推模式回注会话
- 信号量控制并发上限，陈旧检测守护进程保障崩溃恢复
- 统一的 Capability 抽象：调用工具与调用外部 Agent 方式一致

### 3. 异构 Agent 接入（MCP / A2A）

通过 `loom-adapters` 协议适配层，将外部能力统一抽象为内部 `Capability`：

- **Native**：内置工具集（文件、代码、Web、看板、记忆等）
- **MCP**：接入 Model Context Protocol 服务器的工具（已实现骨架）
- **A2A**：接入异构语言实现的外部 Agent（待实现）

无论背后是 Rust 内置工具、MCP Server 还是 Python/Go 编写的 Agent，对编排引擎而言都是统一的 `Capability`，可被任意 Agent 调用。

### 4. 智能上下文管理（Context Engine）

- 基于模型 `context_length` 动态计算压缩阈值
- 分层压缩：先低成本工具修剪，再 LLM 结构化摘要
- 压缩状态随 checkpoint 持久化，resume 后摘要链连贯

### 5. Checkpoint 与人在回路

- 每轮保存 checkpoint，`interrupt` 强制保存并暂停
- 支持从任意 checkpoint 恢复，压缩状态/委派深度/工具集全量恢复
- 多租户物理隔离：`namespace=checkpoint:{tenant}:{user}`

### 6. 记忆与防护

- 三层记忆抽象（Store / Provider / Manager），支持 mem0/honcho 等外部后端
- 工具防护：滑动窗口去重、连续失败上限、循环检测、调用次数上限

---

## 架构概览

```
┌─────────────────────────────────────────────────────────┐
│                    loom-chat (HTTP/SSE)                  │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                    loom-agent (大脑 / 编排)               │
│       AgentLoop · DelegationManager · ContextEngine     │
└──────────────────────────┬──────────────────────────────┘
                           │  统一 Capability 抽象
┌──────────────────────────▼──────────────────────────────┐
│                  loom-adapters (协议适配)                 │
│        Native · MCP(已实现) · A2A(待实现)                │
└──────┬──────────────┬──────────────┬────────────────────┘
       │              │              │
┌──────▼─────┐ ┌──────▼─────┐ ┌──────▼─────┐
│loom-tools  │ │ MCP Server │ │外部 Agent  │
│内置工具集   │ │  (异构工具) │ │ (异构语言) │
└──────┬─────┘ └────────────┘ └────────────┘
       │
┌──────▼──────────┬───────────────┬───────────────────────┐
│ loom-llm        │ loom-memory   │ loom-isolation        │
│ OpenAI 兼容     │ 记忆子系统     │ 协程/进程/容器/Wasm   │
└─────────────────┴───────────────┴───────────┬───────────┘
                                              │
┌─────────────────────────────────────────────▼───────────┐
│                    loom-infra / loom-core                │
│         PostgreSQL · Registry · Bus · Lifecycle          │
└─────────────────────────────────────────────────────────┘
```

### Crate 职责

| Crate | 职责 |
|-------|------|
| `loom-core` | 核心 trait 与类型（Capability、Lifecycle、Isolation、Checkpoint） |
| `loom-agent` | Agent 对话循环与编排（大脑） |
| `loom-adapters` | 协议适配：Native / MCP / A2A，统一为 Capability |
| `loom-tools` | 内置工具集（文件、代码、Web、看板、记忆、技能等） |
| `loom-llm` | LLM Provider 抽象（OpenAI 兼容协议） |
| `loom-memory` | 记忆子系统（curated memory + 外部 provider） |
| `loom-isolation` | 分级隔离后端（协程/进程/容器/WASM） |
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

### 5. 前端界面

```bash
cd web
npm install      # 安装依赖
npm run dev      # 启动开发服务器
```

启动后访问 `http://127.0.0.1:5173`，Vite 会将 `/chat`、`/agents` 等请求代理到后端 `http://127.0.0.1:3000`。

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