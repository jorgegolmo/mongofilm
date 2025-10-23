
# Docker Setup

## Container Creation

```sh
docker pull mongo:7.0.1
docker run -d --name mongofilm mongo:7.0.1
```

## Container Execution

```sh
docker start mongofilm
docker exec -it mongofilm bash
```

## Container Setup

Needed only for developing

```sh
echo "deb http://archive.ubuntu.com/ubuntu jammy main restricted universe multiverse" > /etc/apt/sources.list
echo "deb http://archive.ubuntu.com/ubuntu jammy-updates main restricted universe multiverse" >> /etc/apt/sources.list
echo "deb http://security.ubuntu.com/ubuntu jammy-security main restricted universe multiverse" >> /etc/apt/sources.list
apt update
apt install git gh git-lfs
```
