// Copyright 2020 The VectorQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.
//
// https://github.com/rust-lang/rustfmt/blob/e1ab878ccb24cda1b9e1c48865b375230385fede/build.rs

use std::env;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    if Path::new(".git/HEAD").exists() {
        println!("cargo:rerun-if-changed=.git/HEAD");
    }

    println!("cargo:rerun-if-env-changed=CFG_RELEASE_CHANNEL");
    if option_env!("CFG_RELEASE_CHANNEL").map_or(true, |c| c == "nightly" || c == "dev") {
        println!("cargo:rustc-cfg=nightly");
    }

    create_version_info();
}

fn create_version_info() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    File::create(out_dir.join("version-info.txt"))
        .unwrap()
        .write_all(commit_info().as_bytes())
        .unwrap();
}

// Try to get hash and date of the last commit on a best effort basis. If anything goes wrong
// (git not installed or if this is not a git repository) just return an empty string.
fn commit_info() -> String {
    match (commit_hash(), commit_date()) {
        (Some(hash), Some(date)) => format!(
            "{}-{} ({})",
            option_env!("CARGO_PKG_VERSION").unwrap_or("unknown"),
            hash.trim_end(),
            date
        ),
        _ => String::new(),
    }
}

fn commit_hash() -> Option<String> {
    Command::new("git")
        .args(&["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|r| String::from_utf8(r.stdout).ok())
}

fn commit_date() -> Option<String> {
    Command::new("git")
        .args(&["log", "-1", "--date=short", "--pretty=format:%cd"])
        .output()
        .ok()
        .and_then(|r| String::from_utf8(r.stdout).ok())
}
