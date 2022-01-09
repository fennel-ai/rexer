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

sudo tailscale up --advertise-routes=172.31.0.0/16 --authkey tskey-kgCz7h7CNTRL-sFgK73QSgVehP8xURhceX