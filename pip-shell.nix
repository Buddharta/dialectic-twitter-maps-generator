{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  nativeBuildInputs = [
    pkgs.gfortran
    pkgs.pkg-config
    pkgs.lapack-reference
    pkgs.python310
    pkgs.zlib
    (pkgs.poetry.override { python3 = pkgs.python310; })
  ];
  packages = [
    (pkgs.python3.withPackages (python-pkgs: [
      python-pkgs.pandas
      python-pkgs.requests
      python-pkgs.pymongo
      python-pkgs.flask
      python-pkgs.scikit-image
    ]))
  ];
  shellHook = ''    
    export BLAS="${pkgs.lapack-reference}/lib/libblas.so"
    export LAPACK="${pkgs.lapack-reference}/lib/liblapack.so"
    export LD_LIBRARY_PATH=${pkgs.lib.makeLibraryPath [
      pkgs.stdenv.cc.cc
    ]}
  '';
}



