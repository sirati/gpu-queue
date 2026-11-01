{
  lib,
  rustPlatform,
  python3,
  systemd,
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
  COMPILER_SYSTEMCTL = "${systemd}/bin/systemctl";
  RUST_TEST_THREADS = "4";
  nativeCheckInputs = [ python3 ];
  postCheck = ''
    export GPU_QUEUE_TEST_BIN_DIR=$(dirname "$(find target -type f -name compile-queue -executable -print -quit)")
    ${python3.interpreter} tests/compiler_actor.py
  '';

  postInstall = ''
    mkdir -p $out/share/systemd/user
    cat > $out/share/systemd/user/doggy-compile.service <<EOF
    [Unit]
    Description=Rust compiler queue
    RefuseManualStop=yes
    [Service]
    Type=notify
    NotifyAccess=main
    ExecStart=$out/bin/compile-queue
    Restart=always
    RestartSec=1
    WatchdogSec=60
    TimeoutStartSec=120
    TimeoutStopSec=infinity
    KillMode=process
    KillSignal=SIGTERM
    UMask=0077
    [Install]
    WantedBy=default.target
    EOF
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
