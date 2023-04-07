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
# 10.109.0.0/16: Convoy production plane
# 10.111.0.0/16: Data plane for self serve.
# 10.112.0.0/16: Data plane for lokal in independent account
# 10.113.0.0/16: Data plane for lokal in their organization
# 10.114.0.0/16: Data plane for yext in their account in us-east-1
# 10.121.0.0/16: Data plane for oslash in their account
# 10.122.0.0/16: Data plane for oslash in their account in us-east-1
# 10.123.0.0/16: Demo data plane for feature engineering
sudo tailscale up \
  --advertise-routes=172.31.0.0/16,10.101.0.0/16,10.102.0.0/16,10.103.0.0/16,10.104.0.0/16,10.105.0.0/16,10.109.0.0/16,10.111.0.0/16,10.112.0.0/16,10.121.0.0/16,10.122.0.0/16,10.113.0.0/16,10.114.0.0/16,10.123.0.0/16 \
  --authkey %TAILSCALE_AUTHKEY%
