{ pkgs ? import <nixpkgs> {} }:
let
  unstable = import (fetchTarball https://channels.nixos.org/nixpkgs-unstable/nixexprs.tar.xz) { };
  # Packages for python development
  # We install python 3.9 instead of 3.10 because of a known
  # compatibility issue between the nix version of poetry (1.1.12)
  # and python 3.10 (https://github.com/python-poetry/poetry/issues/4210)
  my-python = pkgs.python39Full;
  python-with-my-packages = my-python.withPackages (p: with p; [
    pandas
    requests
    cloudpickle
    numpy
    pip
    # other python packages you want
  ]);
in

with pkgs; mkShell {
  buildInputs = [

    pkgs.direnv
    pkgs.nix-direnv
    pkgs.git
    pkgs.ssh-agents

    # for xgboost
    pkgs.cmake
    llvmPackages.openmp

    # Protobuf
    pkgs.protobuf

    # Packages for go development.
    unstable.go_1_19
    pkgs.protoc-gen-go
    pkgs.protoc-gen-go-grpc
    unstable.capnproto

    # Packages to build kafka go client
    pkgs.rdkafka
    pkgs.openssl
    pkgs.pkg-config

    # Install act - https://github.com/nektos/act
    # `act` is used to run and test github actions locally.
    pkgs.act

    python-with-my-packages
    pkgs.poetry
    pkgs.pipenv
    # Packages for javascript development
    pkgs.nodejs

    # Packages for deployment
    pkgs.docker

    # Packages for working with kubernetes
    pkgs.kubernetes
    pkgs.kubernetes-helm

    # Needed for linkerd setup and client.
    pkgs.linkerd
    pkgs.step-cli

    # Tools for connecting to dbs
    pkgs.confluent-platform
    pkgs.redis
    pkgs.mysql-client

    # Some nice-to-have tools
    pkgs.jq  # A lightweight and flexible command-line JSON processor
    pkgs.fzf # A command-line fuzzy finder written in Go
    pkgs.fzf-zsh # wrap fzf to use in oh-my-zsh
    pkgs.ripgrep # grep, but faster
    pkgs.delta # A syntax-highlighting pager for git
    pkgs.wget
    pkgs.inetutils
    pkgs.bash

    # Tools to visualize pprof output.
    pkgs.graphviz
  ];
  shellHook =
  ''
    source bash.rc
    export PIP_PREFIX="$(pwd)/_build/pip_packages"
    export PYTHONPATH="$(pwd)/_build/pip_packages/lib/python3.9/site-packages:$PYTHONPATH"
    unset SOURCE_DATE_EPOCH
   '';
}
