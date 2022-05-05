#!/bin/bash

# Following instructions from https://docs.retool.com/docs/enabling-ssh-tunnels
# to set up an SSH tunnel for the Retool servers.

sudo adduser retool --password NP

# Login as root.
sudo su

# Create the authorized_keys file if it does not exist yet
mkdir -p /home/retool/.ssh
touch /home/retool/.ssh/authorized_keys

# Use your favorite editor to add Retool's public key to the file
tee /home/retool/.ssh/authorized_keys <<EOF
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDBsROc66bznCNl7AMtrgxkSBx3hxK4zQt7YYR6KENSb/Uu8a76YQbC9JSArJOiHjA2fxl1ZS+2Zxoj4H3/H3QKIVwIugRs2ayCRy/IoMHa4NDC+eTNXwU+828Q9J9q3BGW6cigHLFWlgtv8kZer0smERPfv/ByvQuW7wY0dLTZx+DwdKvIuz18ngrU3zhABoLd5A10Z2LYBMmte1Bk5H3B0/dTsBofnCdTZytNYAFzVRiovmYTFg9jov3HnvCXnXobozpKfQA+ynNx8f7NxgknRup+Kh8EeNDcNAzJgcC/lGOWYv2xjxoTmFuRUf5M0Bme7TRdEjmxHNz9modYeKe86mOMlCp+VTqSaH5oh5iRoLn7shIdyHx7lSxNmvmSJu6M3nNdg5/hluB6s0IRCOh1e8iBFjn1A0PFaIUO5eZ0+WR6NjRo83f/FF8W8plmvBtUCHOqD8pvc2Xg/2Jkp2tj2iIhKpp6X87yWC62pmkRjNF0WUwwD1EPZYPh9VeYuYncH50UD5ZlpWJv1s/+wYDUrhoJhk1N3nYhR00MNPVF1Z1L0iRg9NdXS3hk6WEQ+65ZqYZoBSE0YAqSHppLVlAcsHU4u3AHBYhgMABboVkgKLWQ3Uk+51pZI86MK+kSNXuRviQICl01Cq4H5Ny172KNhFRcSgbtPxDfSPvA9lKrIw== retool@fennel
EOF

# Allow ssh from abhay's mac.
tee -a /home/retool/.ssh/authorized_keys <<EOF
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDHsj56rzZ0+3Qc3ElWgmdh9patRkk6Xk6BpqM/C2M0Gem51JT1PfE3rCh7PVTk6+9ji4KQQKyLUfEqrF3U/VcbTHGGM5ur/N99QI7IX8v8n5Qlrjyww6wNbdrs4PPc1OKcu8jlhS4oRxVk2ILGyTWGnQ+LXR9/FYaDsGnIHHFvCH286hgBpM7adTJ0/zS+/q+FexF13CrjRLezHbYNhVJx8gA89eNzx6d3KQAFDvyBtvjnC65tnItMHI7V2ew614v4uhFrW2s7zY7XgZlXlUVVTa0BgxKMy0XuUzu5a0ckfGy8OVpRwneWzEL77Hida7tEgBsBhpM4zZKc62zI7NniYdmdR1dlFJna9slQNE4Po8hgkpr6atWcQ7iak9CZh/IwwZRYB3sp4R3BHaviLIAUxgzU/rJe7kP57jvBcqD5Du173Ryc0XwMYJHhRefrn3uOrtpArlJFHVae6/PjUKcd6JPgPofZU5HSHlUkw/L3OKYQfhxZdHpt/xx80Gqrnv8= abhay@Abhays-MacBook-Pro.local
EOF

# Set permissions on the authorized_keys file
chmod 644 /home/retool/.ssh/authorized_keys

# Change owner of authorized_keys file to Retool
chown retool:retool /home/retool/.ssh/authorized_keys


# Install mysql client
sudo yum -y update
sudo yum install -y mariadb