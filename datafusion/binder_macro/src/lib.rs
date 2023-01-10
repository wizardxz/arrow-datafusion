extern crate proc_macro;
use std::str::FromStr;

use proc_macro::TokenStream;
use regex::Regex;

fn uppercase_first_letter(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

#[proc_macro_attribute]
pub fn route(attr: TokenStream, item: TokenStream) -> TokenStream {
    let raw = item.to_string();
    let re = Regex::new(
        r"impl\s+Binder\s*<\s*(\w+)\s*>\s*for\s+(\w+)ContextAll\s*<\s*'_\s*>\s*\{\s*\}",
    )
    .unwrap();
    let caps = re.captures(raw.as_str()).unwrap();

    let semantic_node = caps.get(1).map_or("", |m| m.as_str());
    let syntax_node = caps.get(2).map_or("", |m| m.as_str());

    let alternatives = attr
        .into_iter()
        .map(|x| x.to_string())
        .filter(|x| x != ",")
        .map(|x| {
            let x_with_cap = uppercase_first_letter(x.as_str());
            String::from(format!(
                "{syntax_node}ContextAll::{x_with_cap}Context(c) \
            => c.bind(bc),\n"
            ))
        })
        .collect::<Vec<String>>()
        .join("");

    TokenStream::from_str(
        format!(
            "impl Binder<{semantic_node}> for {syntax_node}ContextAll<'_> {{
                fn bind(&self, bc: &BindingContextStack) -> Result<{semantic_node}> {{
                    match &*self {{
                        {alternatives}
                        _ => Err(DataFusionError::NotImplemented(String::from(
                            \"not implemented bind {semantic_node} for {syntax_node}\",
                        ))),
                    }}
                }}
            }}"
        )
        .as_str(),
    )
    .unwrap()
}
