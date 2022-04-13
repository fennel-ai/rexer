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
# 10.102.0.0/16: Test data plane
# 10.103.0.0/16: Dev data plane
# 10.104.0.0/16: Lokal dev data plane
# 10.105.0.0/16: Lokal prod data plane
# 10.106.0.0/16: Demo data plane
sudo tailscale up --advertise-routes=172.31.0.0/16,10.102.0.0/16,10.103.0.0/16,10.104.0.0/16,10.105.0.0/16,10.106.0.0/16 --authkey tskey-kDewAh5CNTRL-FbviE3gvCANZXyP9teQwh
