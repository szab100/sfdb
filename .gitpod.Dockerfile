FROM gitpod/workspace-full
                    
USER gitpod

# Install custom tools, runtime, etc. using apt-get
# For example, the command below would install "bastet" - a command line tetris clone:
#
# RUN sudo apt-get -q update && #     sudo apt-get install -yq bastet && #     sudo rm -rf /var/lib/apt/lists/*
#
# More information: https://www.gitpod.io/docs/config-docker/
RUN mkdir $HOME/bin
ENV PATH "$PATH:$HOME/bin"
RUN wget https://github.com/bazelbuild/bazel/releases/download/0.29.1/bazel-0.29.1-linux-x86_64 -O $HOME/bin/bazel
RUN wget https://github.com/bazelbuild/buildtools/releases/download/0.29.0/buildifier -O $HOME/bin/buildifier
RUN chmod +x $HOME/bin/*
