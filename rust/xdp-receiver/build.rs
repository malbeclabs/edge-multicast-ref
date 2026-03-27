use std::env;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};

fn main() {
    println!("cargo:rerun-if-changed=ebpf/src");
    println!("cargo:rerun-if-changed=ebpf/Cargo.toml");

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR not set"));

    let endian = env::var("CARGO_CFG_TARGET_ENDIAN").expect("CARGO_CFG_TARGET_ENDIAN not set");
    let bpf_target = if endian == "little" {
        "bpfel-unknown-none"
    } else {
        "bpfeb-unknown-none"
    };

    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").expect("CARGO_CFG_TARGET_ARCH not set");

    let ebpf_dir = PathBuf::from("ebpf");
    let target_dir = out_dir.join("ebpf-target");

    let mut cmd = Command::new("rustup");
    cmd.args([
        "run",
        "nightly",
        "cargo",
        "build",
        "--manifest-path",
    ]);
    cmd.arg(ebpf_dir.join("Cargo.toml"));
    cmd.args([
        "-Z",
        "build-std=core",
        "--bins",
        "--message-format=json",
        "--release",
        "--target",
        bpf_target,
        "--target-dir",
    ]);
    cmd.arg(&target_dir);

    // Set RUSTFLAGS for eBPF target arch
    let rustflags = format!(
        "--cfg=bpf_target_arch=\"{target_arch}\"\x1f-Cdebuginfo=2\x1f-Clink-arg=--btf"
    );
    cmd.env("CARGO_ENCODED_RUSTFLAGS", &rustflags);
    // Remove inherited RUSTC to avoid workspace toolchain conflicts
    cmd.env_remove("RUSTC");
    cmd.env_remove("RUSTC_WORKSPACE_WRAPPER");

    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn eBPF build");

    // Forward stderr as cargo warnings
    let stderr = child.stderr.take().unwrap();
    let stderr_handle = std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            let line = line.expect("read line");
            println!("cargo:warning={line}");
        }
    });

    // Parse JSON messages to find the output binary
    let stdout = child.stdout.take().unwrap();
    let reader = BufReader::new(stdout);
    let mut executables: Vec<(String, PathBuf)> = Vec::new();

    for message in cargo_metadata::Message::parse_stream(reader) {
        match message.expect("valid JSON") {
            cargo_metadata::Message::CompilerArtifact(artifact) => {
                if let Some(executable) = artifact.executable {
                    executables
                        .push((artifact.target.name.clone(), executable.into_std_path_buf()));
                }
            }
            cargo_metadata::Message::CompilerMessage(msg) => {
                for line in msg.message.rendered.unwrap_or_default().split('\n') {
                    println!("cargo:warning={line}");
                }
            }
            _ => {}
        }
    }

    let status = child.wait().expect("failed to wait for eBPF build");
    if !status.success() {
        panic!("eBPF build failed: {status}");
    }

    stderr_handle.join().expect("stderr thread panicked");

    // Copy executables to OUT_DIR
    for (name, binary) in executables {
        let dst = out_dir.join(&name);
        fs::copy(&binary, &dst)
            .unwrap_or_else(|e| panic!("failed to copy {binary:?} to {dst:?}: {e}"));
    }
}
