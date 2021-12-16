# { pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/3590f02e7d5760e52072c1a729ee2250b5560746.tar.gz") {} }:
{ pkgs ? import <nixpkgs> {} }:


pkgs.mkShell {
  buildInputs = [
    pkgs.which
    pkgs.htop
    pkgs.zlib
    pkgs.protobuf3_9
    pkgs.protoc-gen-go
  ];
}
