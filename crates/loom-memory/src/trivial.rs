//! Trivial prompt 过滤
//!
//! 问候/确认类输入不需要召回记忆，避免每轮都触发可能昂贵的向量检索。

/// 判断用户输入是否为 trivial prompt（问候/确认/单字等），
/// 若是则跳过 memory prefetch。
pub fn is_trivial_prompt(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return true;
    }
    // 单字符或极短输入
    if trimmed.chars().count() <= 2 {
        return true;
    }
    let lower = trimmed.to_lowercase();
    // 去除尾部标点后再比较（支持 "thanks!"、"ok." 等）
    let stripped = lower.trim_end_matches(|c: char| c.is_ascii_punctuation() || c == '！' || c == '。' || c == '？');
    // 精确匹配
    let exact = [
        "hi", "hello", "hey", "yo", "sup", "早上好", "你好", "嗨",
        "ok", "okay", "k", "yes", "yep", "y", "no", "nope", "n",
        "sure", "fine", "good", "great", "thanks", "thx",
        "bye", "goodbye", "see you", "再见", "拜拜",
        "继续", "go on", "continue", "next",
        "嗯", "哦", "啊", "好的", "行", "可以", "不错",
    ];
    if exact.contains(&stripped) {
        return true;
    }
    // 问候前缀 + 简短后缀（如 "hello there", "hi!"）
    let greetings = ["hi", "hello", "hey", "yo", "sup", "你好", "早上好"];
    if let Some(g) = greetings.iter().find(|g| lower.starts_with(*g)) {
        let rest = &lower[g.len()..];
        // 后缀只允许标点或 1-2 个词
        return rest.trim().chars().count() <= 8;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_greetings_are_trivial() {
        assert!(is_trivial_prompt("hi"));
        assert!(is_trivial_prompt("hello there"));
        assert!(is_trivial_prompt("你好"));
    }

    #[test]
    fn test_confirmations_are_trivial() {
        assert!(is_trivial_prompt("ok"));
        assert!(is_trivial_prompt("thanks!"));
        assert!(is_trivial_prompt("好的"));
    }

    #[test]
    fn test_substantive_prompts_are_not_trivial() {
        assert!(!is_trivial_prompt("How do I implement a binary search tree in Rust?"));
        assert!(!is_trivial_prompt("帮我写一个斐波那契数列"));
    }

    #[test]
    fn test_empty_is_trivial() {
        assert!(is_trivial_prompt(""));
        assert!(is_trivial_prompt("   "));
    }
}