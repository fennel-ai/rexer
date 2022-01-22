# { pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/3590f02e7d5760e52072c1a729ee2250b5560746.tar.gz") {} }:
{ pkgs ? import <nixpkgs> {} }:


pkgs.mkShell {
  buildInputs = [

    pkgs.direnv

    # Protobuf
    pkgs.protobuf3_9

    # Packages for go development.
    pkgs.go_1_17
    pkgs.protoc-gen-go

    # Packages to build kafka go client
    pkgs.rdkafka
    pkgs.openssl
    pkgs.pkg-config

    # Packages for python development
    pkgs.python310
    pkgs.poetry

    # Packages for javascript development
    pkgs.nodejs

    # Packages for deployment
    pkgs.pulumi-bin
    pkgs.docker
    pkgs.kubernetes
    pkgs.kubernetes-helm
    pkgs.linkerd
    pkgs.step-cli

    # Tools for connecting to dbs
    pkgs.confluent-platform
    pkgs.redis
    pkgs.pscale
    pkgs.mysql-client

    # Some nice-to-have tools
    pkgs.jq
    pkgs.fzf
    pkgs.fzf-zsh
    pkgs.htop
    pkgs.zlib
  ];
}
