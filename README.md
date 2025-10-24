
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

Needed only for developing.

```sh
echo "deb http://archive.ubuntu.com/ubuntu jammy main restricted universe multiverse" > /etc/apt/sources.list
echo "deb http://archive.ubuntu.com/ubuntu jammy-updates main restricted universe multiverse" >> /etc/apt/sources.list
echo "deb http://security.ubuntu.com/ubuntu jammy-security main restricted universe multiverse" >> /etc/apt/sources.list
apt update
apt install git gh
```

# Repository Setup

First, you need to clone the repository.

```sh
git clone https://github.com/jorgegolmo/mongofilm.git
```

Then you need to download the CSV data from the course Blackboard page, and put it in dat/origin inside of the repository root.

After that, (optionally) create the virtual environment and install the project requirements:

```sh
# Optional
python -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

To obtain the clean CSVs, run the EDA notebook or its synced Python file:

```sh
python src/eda.py
```
