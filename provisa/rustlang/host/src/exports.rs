// The code's tagged functions: provisa/rustlang/build.py writes this file in every build, with
// each of them; the host as it ships has none.
use crate::glue::Export;

pub fn exports() -> Vec<(&'static str, Export)> {
    Vec::new()
}
