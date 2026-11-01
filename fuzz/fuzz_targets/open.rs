#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| solistore::fuzzing::open(data));
