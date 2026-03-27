fn main() {
    println!("cargo:rerun-if-changed=ebpf/src");
    println!("cargo:rerun-if-changed=ebpf/Cargo.toml");

    // Only build eBPF on Linux (requires nightly + bpf-linker)
    #[cfg(target_os = "linux")]
    {
        use std::path::PathBuf;

        let ebpf_dir = PathBuf::from("ebpf");

        let metadata = cargo_metadata::MetadataCommand::new()
            .manifest_path(ebpf_dir.join("Cargo.toml"))
            .no_deps()
            .exec()
            .expect("failed to get ebpf crate metadata");

        let ebpf_package = metadata
            .packages
            .iter()
            .find(|p| p.name == "xdp-filter")
            .expect("xdp-filter package not found in ebpf/Cargo.toml");

        let pkg = aya_build::Package {
            name: ebpf_package.name.clone(),
            root_dir: ebpf_package
                .manifest_path
                .parent()
                .unwrap()
                .as_std_path()
                .to_path_buf(),
            ..Default::default()
        };

        aya_build::build_ebpf(vec![pkg], aya_build::Toolchain::default())
            .expect("failed to build eBPF program");
    }

    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("cargo:warning=Skipping eBPF build on non-Linux platform. XDP features will not be available.");
    }
}
