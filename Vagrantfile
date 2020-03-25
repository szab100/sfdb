# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/bionic64"
  config.vm.box_version = "= 20200325.0.0"
  config.vm.synced_folder ".", "/src"
  config.vm.network "forwarded_port", guest: 12987, host: 8000, host_ip: "127.0.0.1"

  # Work around disconnected virtual network cable.
  config.vm.provider "virtualbox" do |vb|
    vb.name = "sfdb_build"

    # Configure resources dedicated to the VM
    vb.memory = "6144"
    vb.cpus = 2

    vb.customize ["modifyvm", :id, "--cableconnected1", "on"]
  end

  config.vm.provision "shell", inline: <<-SHELL
    apt-get -qqy update

    apt-get -qqy install make zip unzip git pkg-config libssl-dev zlib1g-dev

    echo "[vagrant provisioning] Downloading Bazel..."
    wget --quiet https://github.com/bazelbuild/bazel/releases/download/0.29.1/bazel-0.29.1-installer-linux-x86_64.sh
   
    echo "[vagrant provisioning] Installing Bazel"
    chmod +x ./bazel-0.29.1-installer-linux-x86_64.sh
    ./bazel-0.29.1-installer-linux-x86_64.sh

    echo "Done installing your virtual machine!"
  SHELL
end
