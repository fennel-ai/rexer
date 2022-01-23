# { pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/3590f02e7d5760e52072c1a729ee2250b5560746.tar.gz") {} }:
{ pkgs ? import <nixpkgs> {} }:


pkgs.mkShell {
  buildInputs = [

    pkgs.direnv
    pkgs.nix-direnv

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
    pkgs.aws

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
    pkgs.pscale
    pkgs.mysql-client

    # Some nice-to-have tools
    pkgs.jq  # A lightweight and flexible command-line JSON processor
    pkgs.fzf # A command-line fuzzy finder written in Go
    pkgs.fzf-zsh # wrap fzf to use in oh-my-zsh
    pkgs.ripgrep # grep, but faster
    pkgs.delta # A syntax-highlighting pager for git
  ];
  shellHook =
  ''
    source bash.rc
  '';
}
