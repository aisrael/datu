use wasm_bindgen::prelude::*;

pub mod api;
pub mod error;
pub mod formats;
pub mod schema;

#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();
}
