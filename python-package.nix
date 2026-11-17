{
  lib,
  buildPythonPackage,
  rustPlatform,
  gpu-queue,
  python,
}:
buildPythonPackage {
  pname = "gpu-queue";
  version = "0.1.0";
  pyproject = true;
  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [ ./Cargo.toml ./Cargo.lock ./src ./examples ./pyproject.toml ./python ./tests/test_python_shutdown.py ./tests/test_python_compiler.py ];
  };
  cargoDeps = rustPlatform.importCargoLock { lockFile = ./Cargo.lock; };
  nativeBuildInputs = [ rustPlatform.cargoSetupHook rustPlatform.maturinBuildHook ];
  maturinBuildFlags = [ "--features" "python" ];
  # Read by rustc (option_env!), so it must be exported: with structured attributes a plain one is not.
  env.GPU_QUEUE_BIN = "${gpu-queue}/bin";
  doCheck = true;
  checkPhase = ''
    runHook preCheck
    export GPU_QUEUE_TEST_BIN_DIR=${gpu-queue}/bin
    export GPU_QUEUE_TEST_PACKAGE_BIN=$GPU_QUEUE_BIN
    export GPU_QUEUE_TEST_NVML=${gpu-queue}/libexec/gpu-queue/libnvml_mock.so
    export GPU_QUEUE_TEST_EXTENSION=$(find "$out" -name '_native*.so' -print -quit)
    ${python.interpreter} -m unittest discover -s tests -p test_python_shutdown.py -v
    ${python.interpreter} -m unittest discover -s tests -p test_python_compiler.py -v
    runHook postCheck
  '';
  pythonImportsCheck = [ "gpu_queue" ];
  meta.platforms = lib.platforms.linux;
}
