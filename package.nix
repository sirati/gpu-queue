{
  lib,
  rustPlatform,
}:

rustPlatform.buildRustPackage {
  pname = "gpu-queue";
  version = "0.1.0";
  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [
      ./Cargo.toml
      ./Cargo.lock
      ./src
      ./tests
    ];
  };
  cargoLock.lockFile = ./Cargo.lock;
  RUST_TEST_THREADS = "4";

  postInstall = ''
    mkdir -p $out/share/bash-completion/completions
    $out/bin/gpu-run completions bash > $out/share/bash-completion/completions/gpu-run
  '';

  # checkPhase runs the integration tests: daemon + ninfer-mock on loopback,
  # nvidia_smi null or faked by ninfer-mock. No GPU, no network.

  meta = {
    description = "Shares one GPU between ninfer-serve and queued jobs, behind a holding proxy";
    platforms = lib.platforms.linux;
    mainProgram = "gpu-queue";
  };
}
