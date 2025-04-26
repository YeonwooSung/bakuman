use std::collections::HashSet;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TopicPath {
    segments: Vec<String>,
}

impl TopicPath {
    pub fn new(path: &str) -> Self {
        let segments = path.split('/').map(String::from).collect();
        Self { segments }
    }

    pub fn matches(&self, pattern: &TopicPath) -> bool {
        if pattern.segments.len() != self.segments.len() {
            return false;
        }

        for (pat, seg) in pattern.segments.iter().zip(self.segments.iter()) {
            if pat != "*" && pat != ">" && pat != seg {
                return false;
            }
        }

        true
    }

    pub fn to_string(&self) -> String {
        self.segments.join("/")
    }
}

#[derive(Debug, Clone)]
pub struct Subscription {
    pub client_id: String,
    pub topic_patterns: HashSet<TopicPath>,
}
