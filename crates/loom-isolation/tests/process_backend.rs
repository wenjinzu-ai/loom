//! 进程隔离后端端到端测试
//!
//! 使用已编译的 loom-chat 二进制作为 Guest，验证：
//! - ProcessBackend 能正确启动子进程
//! - 健康检查能正确反映进程状态
//! - 发送 stop 后 Guest 能退出（返回 output 或错误）
//!
//! 注意：这些测试会启动真实子进程并占用 LLM/DB 资源，需串行执行。

use loom_core::{AgentSpec, IsolationBackend, IsolationLevel};
use loom_isolation::ProcessBackend;
use std::path::PathBuf;
use std::sync::Mutex;

/// 全局锁，确保进程隔离测试串行执行（避免共享 LLM/DB 资源竞争）
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn loom_chat_binary() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p.push("target");
    p.push("debug");
    p.push(if cfg!(windows) {
        "loom-chat.exe"
    } else {
        "loom-chat"
    });
    p
}

fn project_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p
}

fn make_spec(binary: &PathBuf, goal: &str) -> AgentSpec {
    let config_path = project_root().join("config.toml");
    AgentSpec {
        agent_id: uuid::Uuid::new_v4(),
        capability_id: None,
        goal: goal.into(),
        context: String::new(),
        toolsets: vec![],
        isolation: IsolationLevel::Process,
        timeout: None,
        config: serde_json::json!({
            "process_binary": binary.to_str().unwrap(),
            "process_args": ["--config", config_path.to_str().unwrap()],
        }),
        parent_agent_id: None,
        delegate_depth: 0,
        scope: None,
        parent_toolsets: vec![],
        session_id: None,
        activity_state: None,
    }
}

/// 验证 Guest 启动后健康检查通过
#[tokio::test(flavor = "multi_thread")]
async fn process_backend_health_after_spawn() {
    let _guard = TEST_LOCK.lock().unwrap();
    let binary = loom_chat_binary();
    if !binary.exists() {
        eprintln!("skipping: loom-chat binary not found at {}", binary.display());
        return;
    }

    let backend = ProcessBackend::new();
    let spec = make_spec(&binary, "test");

    let runtime = backend.spawn(spec).await.expect("spawn should succeed");

    // 等待 Guest 启动，期间进程应存活（轮询以容忍资源竞争导致的启动延迟）
    let mut alive = false;
    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if runtime.health().await.alive {
            alive = true;
            break;
        }
    }
    assert!(alive, "process should be alive after spawn");

    // 清理：停止进程
    let _ = runtime.stop().await;
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        runtime.wait(),
    )
    .await;
}

/// 验证发送 stop 后 Guest 能退出并返回结果（output 或错误）
#[tokio::test(flavor = "multi_thread")]
async fn process_backend_stop_terminates() {
    let _guard = TEST_LOCK.lock().unwrap();
    let binary = loom_chat_binary();
    if !binary.exists() {
        eprintln!("skipping: loom-chat binary not found at {}", binary.display());
        return;
    }

    let backend = ProcessBackend::new();
    let spec = make_spec(&binary, "Write a very long essay.");

    let runtime = backend.spawn(spec).await.expect("spawn should succeed");

    // 等待 Guest 完成 AppState::build 并进入主循环
    tokio::time::sleep(std::time::Duration::from_secs(18)).await;
    runtime.stop().await.expect("stop should succeed");

    // stop 后 Guest 应在宽限期内退出（无论是否产出 output）
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        runtime.wait(),
    )
    .await;

    // 要么成功返回 output，要么返回错误（进程退出无 output），但不能超时
    assert!(
        result.is_ok(),
        "guest should exit within 30s after stop"
    );

    // 验证进程已退出（重试几次以容忍 stdout 关闭与进程退出的时序差）
    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let health = runtime.health().await;
        if !health.alive {
            break;
        }
    }
    let health = runtime.health().await;
    assert!(!health.alive, "process should be dead after stop+wait");
}