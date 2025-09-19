use std::collections::HashSet;

use tantivy::tokenizer::{
    NgramTokenizerWithChars, TokenCharType, TokenCharsConfig, TokenStream, Tokenizer,
};

fn main() {
    println!("=== Ngram Tokenizer with Token Chars Example ===\n");

    // Example 1: Default behavior - all characters included
    println!("1. Default (all characters):");
    let config = TokenCharsConfig::default();
    let mut tokenizer = NgramTokenizerWithChars::new(3, 3, false, config).unwrap();
    demonstrate_tokenization(&mut tokenizer, "hello-world");

    // Example 2: Letters and digits only
    println!("\n2. Alphanumeric only:");
    let config = TokenCharsConfig::default().with_letters().with_digits();
    let mut tokenizer = NgramTokenizerWithChars::new(3, 3, false, config).unwrap();
    demonstrate_tokenization(&mut tokenizer, "hello-world123");

    // Example 3: Letters only
    println!("\n3. Letters only:");
    let mut token_chars = HashSet::new();
    token_chars.insert(TokenCharType::Letter);
    let config = TokenCharsConfig {
        token_chars,
        custom_token_chars: String::new(),
    };
    let mut tokenizer = NgramTokenizerWithChars::new(3, 3, false, config).unwrap();
    demonstrate_tokenization(&mut tokenizer, "hello-world-123");

    // Example 4: With custom characters
    println!("\n4. Letters, digits, and custom characters (-, _):");
    let mut token_chars = HashSet::new();
    token_chars.insert(TokenCharType::Letter);
    token_chars.insert(TokenCharType::Digit);
    token_chars.insert(TokenCharType::Custom);
    let config = TokenCharsConfig {
        token_chars,
        custom_token_chars: "-_".to_string(),
    };
    let mut tokenizer = NgramTokenizerWithChars::new(3, 3, false, config).unwrap();
    demonstrate_tokenization(&mut tokenizer, "hello-world_123");

    // Example 5: Simulating Elasticsearch behavior
    println!("\n5. Elasticsearch-style (letters only, like ES token_chars: ['letter']):");
    let mut token_chars = HashSet::new();
    token_chars.insert(TokenCharType::Letter);
    let config = TokenCharsConfig {
        token_chars,
        custom_token_chars: String::new(),
    };
    let mut tokenizer = NgramTokenizerWithChars::new(3, 3, false, config).unwrap();
    demonstrate_tokenization(&mut tokenizer, "user@example.com");

    // Example 6: Prefix-only mode
    println!("\n6. Prefix-only mode (for autocomplete):");
    let config = TokenCharsConfig::default().with_letters().with_digits();
    let mut tokenizer = NgramTokenizerWithChars::new(3, 3, true, config).unwrap();
    demonstrate_tokenization(&mut tokenizer, "search");

    // Example 7: Unicode handling
    println!("\n7. Unicode handling:");
    let config = TokenCharsConfig::default().with_letters().with_digits();
    let mut tokenizer = NgramTokenizerWithChars::new(3, 3, false, config).unwrap();
    demonstrate_tokenization(&mut tokenizer, "café-2024");

    // Example 8: URL tokenization
    println!("\n8. URL tokenization (custom config for URLs):");
    let mut token_chars = HashSet::new();
    token_chars.insert(TokenCharType::Letter);
    token_chars.insert(TokenCharType::Digit);
    token_chars.insert(TokenCharType::Custom);
    let config = TokenCharsConfig {
        token_chars,
        custom_token_chars: "-./:".to_string(),
    };
    let mut tokenizer = NgramTokenizerWithChars::new(3, 3, false, config).unwrap();
    demonstrate_tokenization(&mut tokenizer, "https://example.com");
}

fn demonstrate_tokenization(tokenizer: &mut NgramTokenizerWithChars, text: &str) {
    println!("  Input: \"{}\"", text);

    let mut stream = tokenizer.token_stream(text);
    let mut tokens = Vec::new();

    while stream.advance() {
        let token = stream.token();
        tokens.push((token.text.clone(), token.offset_from, token.offset_to));
    }

    if tokens.is_empty() {
        println!("  No tokens generated");
    } else {
        println!("  Tokens ({}): ", tokens.len());
        for (i, (text, from, to)) in tokens.iter().enumerate() {
            if i < 10 {
                // Show first 10 tokens
                println!("    \"{}\" [{}, {})", text, from, to);
            } else if i == 10 {
                println!("    ... and {} more", tokens.len() - 10);
                break;
            }
        }
    }
}
