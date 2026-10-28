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
    fileset = lib.fileset.unions [ ./Cargo.toml ./Cargo.lock ./src ./pyproject.toml ./python ./tests/test_python_shutdown.py ./tests/test_python_compiler.py ];
  };
  cargoDeps = rustPlatform.importCargoLock { lockFile = ./Cargo.lock; };
  nativeBuildInputs = [ rustPlatform.cargoSetupHook rustPlatform.maturinBuildHook ];
  maturinBuildFlags = [ "--features" "python" ];
  GPU_QUEUE_DAEMON = "${gpu-queue}/bin/gpu-switch";
  doCheck = true;
  checkPhase = ''
    runHook preCheck
    export GPU_QUEUE_TEST_BIN_DIR=${gpu-queue}/bin
    export GPU_QUEUE_TEST_EXTENSION=$(find "$out" -name '_native*.so' -print -quit)
    ${python.interpreter} -m unittest discover -s tests -p test_python_shutdown.py -v
    ${python.interpreter} -m unittest discover -s tests -p test_python_compiler.py -v
    runHook postCheck
  '';
  pythonImportsCheck = [ "gpu_queue" ];
  meta.platforms = lib.platforms.linux;
}
