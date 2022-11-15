#!/bin/bash


# Enable IP-forwarding.
echo 'net.ipv4.ip_forward = 1' | sudo tee -a /etc/sysctl.conf
echo 'net.ipv6.conf.all.forwarding = 1' | sudo tee -a /etc/sysctl.conf
sudo sysctl -p /etc/sysctl.conf


sudo yum -y update gnupg2

sudo yum -y install yum-utils

sudo yum-config-manager --add-repo https://pkgs.tailscale.com/stable/amazon-linux/2/tailscale.repo

sudo yum -y install tailscale

sudo systemctl enable --now tailscaled

# TODO: Don't save the tailscale authkey in plaintext.
# 172.31.0.0/16 : Control plane
# 10.101.0.0/16: Test data plane for feature engineering world
# 10.102.0.0/16: Test data plane
# 10.103.0.0/16: Dev data plane
# 10.105.0.0/16: Lokal prod data plane
# 10.106.0.0/16: Lokal staging data plane
# 10.107.0.0/16: Dev multi-arch test plane
# 10.109.0.0/16: Convoy production plane
# 10.110.0.0/16: Temporary Test plane
# 10.111.0.0/16: Data plane for self serve.
sudo tailscale up \
  --advertise-routes=172.31.0.0/16,10.101.0.0/16,10.102.0.0/16,10.103.0.0/16,10.105.0.0/16,10.106.0.0/16,10.107.0.0/16,10.109.0.0/16,10.110.0.0/16,10.111.0.0/16 \
  --authkey tskey-krRB5i7CNTRL-bgqwFPeYVV2o7bqXUA4KM3
