extern crate cc;
extern crate bindgen;
extern crate glob;
extern crate pkg_config;

use std::{env, path::{Path, PathBuf}, fs};

const ROCKSDB_VERSION: &'static str = "5.14.2";
// FIXME the constants below are wrong
const SNAPPY_VERSION: &'static str = "1.1.7";
const LZ4_VERSION: &'static str = "";
const ZSTD_VERSION: &'static str = "";
const ZLIB_VERSION: &'static str = "";
const BZIP2_VERSION: &'static str = "";

// https://github.com/pingcap/grpc-rs/blob/fead789c219b011ffbba46d2794cc67cdcbdfd4e/grpc-sys/build.rs#L156
fn get_env(name: &str) -> Option<String> {
    println!("cargo:rerun-if-env-changed={}", name);
    match env::var(name) {
        Ok(s) => Some(s),
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(s)) => {
            panic!("unrecognize env var of {}: {:?}", name, s.to_string_lossy());
        }
    }
}

// https://github.com/pingcap/grpc-rs/blob/fead789c219b011ffbba46d2794cc67cdcbdfd4e/grpc-sys/build.rs#L28
fn probe_library(
    library: &str,
    at_least_version: &str,
    cargo_metadata: bool
) -> Result<pkg_config::Library, String> {
    match pkg_config::Config::new()
        .atleast_version(at_least_version)
        .cargo_metadata(cargo_metadata)
        .probe(library)
    {
        Ok(lib) => Ok(lib),
        Err(e) => Err(format!("Cannot find library {} via pkg-config: {:?}", library, e)),
    }
}

fn link(name: &str, bundled: bool) {
    use std::env::var;
    let target = var("TARGET").unwrap();
    let target: Vec<_> = target.split('-').collect();
    if target.get(2) == Some(&"windows") {
        println!("cargo:rustc-link-lib=dylib={}", name);
        if bundled && target.get(3) == Some(&"gnu") {
            let dir = var("CARGO_MANIFEST_DIR").unwrap();
            println!("cargo:rustc-link-search=native={}/{}", dir, target[0]);
        }
    }
}

fn fail_on_empty_directory(name: &str) {
    if fs::read_dir(name).unwrap().count() == 0 {
        println!(
            "The `{}` directory is empty, did you forget to pull the submodules?",
            name
        );
        println!("Try `git submodule update --init --recursive`");
        panic!();
    }
}

fn bindgen_rocksdb(rocksdb_lib_info: Option<pkg_config::Library>) {
    // FIXME make note aboue stupid buildroot hack
    let include_paths = if let Some(lib_info) = rocksdb_lib_info {
        let mut include_paths: Vec<_> = lib_info
            .include_paths
            .iter()
            .map(|path| path.to_str().expect("Failed to stringify include path").to_owned())
            .collect();
        let sysroot = &get_env("PKG_CONFIG_SYSROOT_DIR").unwrap();
        let buildroot_staging_dir = Path::new(sysroot)
            .parent()
            .expect("Failed to get buildroot output directory")
            .join("staging");
        if buildroot_staging_dir.is_dir() {
            include_paths.extend(lib_info.include_paths.iter().filter_map(|path| {
                if path.starts_with(sysroot) {
                    Some(
                        buildroot_staging_dir
                            .join(path.strip_prefix(sysroot).unwrap())
                            .to_str()
                            .unwrap()
                            .to_owned()
                    )
                } else {
                    None
                }
            }));
        }

        include_paths
    } else {
        vec!["rocksdb/include".into()]
    };

    // FIXME recursively include all the rocksdb files if we are building rocksdb ourselves.
    println!("cargo:rerun-if-changed=rocksdb/include/rocksdb/c.h");
    println!("cargo:rerun-if-changed=rocksdb/db/c.cc");
    println!("cargo:rerun-if-changed=rocksdb/utilities/backupable/backupable_db.cc");

    let bindings = include_paths
        .iter()
        .fold(bindgen::Builder::default(), |builder, include_path| {
            builder.clang_arg(format!("-I{}", include_path))
        })
        .header("rocksdb/include/rocksdb/c.h")
        .blacklist_type("max_align_t") // https://github.com/rust-lang-nursery/rust-bindgen/issues/550
        .ctypes_prefix("libc")
        .generate()
        .expect("unable to generate rocksdb bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("unable to write rocksdb bindings");
}

fn build_rocksdb() {
    let mut config = cc::Build::new();
    config.include("rocksdb/include/");
    config.include("rocksdb/");
    config.include("rocksdb/third-party/gtest-1.7.0/fused-src/");

    if cfg!(feature = "snappy") {
        config.define("SNAPPY", Some("1"));
        config.include("snappy/");
    }

    if cfg!(feature = "lz4") {
        config.define("LZ4", Some("1"));
        config.include("lz4/lib/");
    }

    if cfg!(feature = "zstd") {
        config.define("ZSTD", Some("1"));
        config.include("zstd/lib/");
        config.include("zstd/lib/dictBuilder/");
    }

    if cfg!(feature = "zlib") {
        config.define("ZLIB", Some("1"));
        config.include("zlib/");
    }

    if cfg!(feature = "bzip2") {
        config.define("BZIP2", Some("1"));
        config.include("bzip2/");
    }

    config.include(".");
    config.define("NDEBUG", Some("1"));

    let mut lib_sources = include_str!("rocksdb_lib_sources.txt")
        .split(" ")
        .collect::<Vec<&'static str>>();

    // We have a pregenerated a version of build_version.cc in the local directory
    lib_sources = lib_sources
        .iter()
        .cloned()
        .filter(|file| *file != "util/build_version.cc")
        .collect::<Vec<&'static str>>();

    if cfg!(target_os = "macos") {
        config.define("OS_MACOSX", Some("1"));
        config.define("ROCKSDB_PLATFORM_POSIX", Some("1"));
        config.define("ROCKSDB_LIB_IO_POSIX", Some("1"));

    }
    if cfg!(target_os = "linux") {
        config.define("OS_LINUX", Some("1"));
        config.define("ROCKSDB_PLATFORM_POSIX", Some("1"));
        config.define("ROCKSDB_LIB_IO_POSIX", Some("1"));
        // COMMON_FLAGS="$COMMON_FLAGS -fno-builtin-memcmp"
    }
    if cfg!(target_os = "freebsd") {
        config.define("OS_FREEBSD", Some("1"));
        config.define("ROCKSDB_PLATFORM_POSIX", Some("1"));
        config.define("ROCKSDB_LIB_IO_POSIX", Some("1"));
    }

    if cfg!(target_os = "windows") {
        link("rpcrt4", false);
        link("shlwapi", false);
        config.define("OS_WIN", Some("1"));

        // Remove POSIX-specific sources
        lib_sources = lib_sources
            .iter()
            .cloned()
            .filter(|file| match *file {
                "port/port_posix.cc" |
                "env/env_posix.cc" |
                "env/io_posix.cc" => false,
                _ => true,
            })
            .collect::<Vec<&'static str>>();

        // Add Windows-specific sources
        lib_sources.push("port/win/port_win.cc");
        lib_sources.push("port/win/env_win.cc");
        lib_sources.push("port/win/env_default.cc");
        lib_sources.push("port/win/win_logger.cc");
        lib_sources.push("port/win/io_win.cc");
        lib_sources.push("port/win/win_thread.cc");
    }

    if cfg!(target_env = "msvc") {
        config.flag("-EHsc");
    } else {
        config.flag("-std=c++11");
        // this was breaking the build on travis due to
        // > 4mb of warnings emitted.
        config.flag("-Wno-unused-parameter");
    }

    for file in lib_sources {
        let file = "rocksdb/".to_string() + file;
        config.file(&file);
    }

    config.file("build_version.cc");

    config.cpp(true);
    config.compile("librocksdb.a");
}

fn build_snappy() {
    let mut config = cc::Build::new();
    config.include("snappy/");
    config.include(".");

    config.define("NDEBUG", Some("1"));

    if cfg!(target_env = "msvc") {
        config.flag("-EHsc");
    } else {
        config.flag("-std=c++11");
    }

    config.file("snappy/snappy.cc");
    config.file("snappy/snappy-sinksource.cc");
    config.file("snappy/snappy-c.cc");
    config.cpp(true);
    config.compile("libsnappy.a");
}

fn build_lz4() {
    let mut compiler = cc::Build::new();

    compiler
        .file("lz4/lib/lz4.c")
        .file("lz4/lib/lz4frame.c")
        .file("lz4/lib/lz4hc.c")
        .file("lz4/lib/xxhash.c");

    compiler.opt_level(3);

    match env::var("TARGET").unwrap().as_str()
    {
      "i686-pc-windows-gnu" => {
        compiler.flag("-fno-tree-vectorize");
      },
      _ => {}
    }

    compiler.compile("liblz4.a");
}

fn build_zstd() {
    let mut compiler = cc::Build::new();

    compiler.include("zstd/lib/");
    compiler.include("zstd/lib/common");
    compiler.include("zstd/lib/legacy");

    let globs = &[
        "zstd/lib/common/*.c",
        "zstd/lib/compress/*.c",
        "zstd/lib/decompress/*.c",
        "zstd/lib/dictBuilder/*.c",
        "zstd/lib/legacy/*.c",
    ];

    for pattern in globs {
        for path in glob::glob(pattern).unwrap() {
            let path = path.unwrap();
            compiler.file(path);
        }
    }

    compiler.opt_level(3);

    compiler.define("ZSTD_LIB_DEPRECATED", Some("0"));
    compiler.compile("libzstd.a");
}

fn build_zlib() {
    let mut compiler = cc::Build::new();

    let globs = &[
        "zlib/*.c"
    ];

    for pattern in globs {
        for path in glob::glob(pattern).unwrap() {
            let path = path.unwrap();
            compiler.file(path);
        }
    }

    compiler.opt_level(3);
    compiler.compile("libz.a");
}

fn build_bzip2() {
    let mut compiler = cc::Build::new();

    compiler
        .file("bzip2/blocksort.c")
        .file("bzip2/bzlib.c")
        .file("bzip2/compress.c")
        .file("bzip2/crctable.c")
        .file("bzip2/decompress.c")
        .file("bzip2/huffman.c")
        .file("bzip2/randtable.c");

    compiler
        .define("_FILE_OFFSET_BITS", Some("64"))
        .define("BZ_NO_STDIO", None);

    compiler.opt_level(3);
    compiler.compile("libbz2.a");
}

// FIXME clean this mess up
fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    let use_pkg_config = get_env("LIBROCKSDB_SYS_USE_PKG_CONFIG").map_or(false, |s| s == "1");

    // FIXME remove?
    if let Some(link_search_native_dir) = get_env("LIBROCKSDB_SYS_LINK_SEARCH_NATIVE_DIR") {
        println!("cargo:rustc-link-search-native={}", link_search_native_dir);
    }

    println!("cargo:rerun-if-changed=rocksdb/");
    fail_on_empty_directory("rocksdb");

    let rocksdb_lib_info = if use_pkg_config {
        match probe_library("librocksdb", ROCKSDB_VERSION, true) {
            Ok(lib_info) => Some(lib_info),
            Err(err) => panic!("{}", err)
        }
    } else {
        None
    };
    bindgen_rocksdb(rocksdb_lib_info);

    if !use_pkg_config {
        build_rocksdb();
    }

    if cfg!(feature = "snappy") {
        if !use_pkg_config {
            fail_on_empty_directory("snappy");
            println!("cargo:rerun-if-changed=snappy/");
            build_snappy();
        } else if let Err(err) = probe_library("libsnappy", SNAPPY_VERSION, true) {
            eprintln!("{}", err);
            println!("cargo:rustc-link-lib=dylib=snappy");
        }
    }

    if cfg!(feature = "lz4") {
        if !use_pkg_config {
            fail_on_empty_directory("lz4");
            println!("cargo:rerun-if-changed=lz4/");
            build_lz4();
        } else if let Err(err) = probe_library("liblz4", LZ4_VERSION, true) {
            eprintln!("{}", err);
            println!("cargo:rustc-link-lib=dylib=lz4");
        }
    }

    if cfg!(feature = "zstd") {
        if !use_pkg_config {
            fail_on_empty_directory("zstd");
            println!("cargo:rerun-if-changed=zstd/");
            build_zstd();
        } else if let Err(err) = probe_library("libzstd", ZSTD_VERSION, true) {
            eprintln!("{}", err);
            println!("cargo:rustc-link-lib=dylib=zstd");
        }
    }

    if cfg!(feature = "zlib") {
        if !use_pkg_config {
            fail_on_empty_directory("zlib");
            println!("cargo:rerun-if-changed=zlib/");
            build_zlib();
        } else if let Err(err) = probe_library("libzlib", ZLIB_VERSION, true) {
            eprintln!("{}", err);
            println!("cargo:rustc-link-lib=dylib=zlib");
        }
    }

    if cfg!(feature = "bzip2") {
        if !use_pkg_config {
            fail_on_empty_directory("bzip2");
            println!("cargo:rerun-if-changed=bzip2/");
            build_bzip2();
        } else if let Err(err) = probe_library("libbzip2", BZIP2_VERSION, true) {
            eprintln!("{}", err);
            println!("cargo:rustc-link-lib=dylib=bzip2");
        }
    }
}
