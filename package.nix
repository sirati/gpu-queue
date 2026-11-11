{
  lib,
  rustPlatform,
  python3,
  systemd,
  autoAddDriverRunpath,
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
      ./examples
    ];
  };
  cargoLock.lockFile = ./Cargo.lock;
  # A daemon given NVML by name (`libnvidia-ml.so.1`) finds the host driver's library on NixOS.
  nativeBuildInputs = [ autoAddDriverRunpath ];
  COMPILER_SYSTEMCTL = "${systemd}/bin/systemctl";
  RUST_TEST_THREADS = "4";
  nativeCheckInputs = [ python3 ];
  postCheck = ''
    export GPU_QUEUE_TEST_BIN_DIR=$(dirname "$(find target -type f -name compile-queue -executable -print -quit)")
    ${python3.interpreter} tests/compiler_actor.py
  '';

  postInstall = ''
    # The mock NVML library the Python package's tests hand their daemons.
    install -Dm755 "$(find target -path '*/examples/libnvml_mock.so' -print -quit)" $out/libexec/gpu-queue/libnvml_mock.so
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
  # NVML null or the mock library (examples/nvml_mock.rs). No GPU, no network.

  meta = {
    description = "Shares one GPU between ninfer-serve and queued jobs, behind a holding proxy";
    platforms = lib.platforms.linux;
    mainProgram = "gpu-queue";
  };
}
