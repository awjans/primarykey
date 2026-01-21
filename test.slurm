#!/usr/bin/env bash
#SBATCH --job-name=primarykey_run
#SBATCH --output=primarykey_run.out
#SBATCH --error=primarykey_run.err
#SBATCH --time=00:00:10
#SBATCH --ntasks=2
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G

if [ -f ~/src/primarykey/.env ]; then
    echo "Sourcing environment variables from .env file"
    source ~/src/primarykey/.env
else
    echo ".env file not found, proceeding without it."
fi

export POSTGRES_DB=${DB_NAME:=testdb}
export POSTGRES_USER=${DB_USER:=postgres}
export POSTGRES_PASSWORD=${DB_PASSWORD:=password}

if [ "${HOSTNAME}" != "ArchChancellor" ]; then
    module load tools/Singularity/latest
    module load module load lang/Python/3.13.5-GCCcore-14.3.0
fi

mkdir -p /tmp/pgdata /tmp/pgrun
if [ ${?} -ne 0 ]; then
    echo "Failed to create temporary directories."
    exit 1
fi

singularity pull --disable-cache --dir /tmp --force postgres.sif docker://postgres:18.1
if [ ${?} -ne 0 ]; then
    echo "Failed to pull PostgreSQL Singularity image."
    exit 1
fi

singularity run -B /tmp/pgdata:/var/lib/postgresql/18 -B /tmp/pgrun:/var/run/postgresql /tmp/postgres.sif
export POSTGRES_PID=${$}
if [ -z "${POSTGRES_PID}" ]; then
    echo "Failed to start PostgreSQL in Singularity container."
    exit 1
fi
echo "Started PostgreSQL with PID ${POSTGRES_PID}"

if [ "${HOSTNAME}" != "ArchChancellor" ]; then
    export OPERATIONS=16000000
fi

if [ -f  ~/src/primarykey/test.sh ]; then
    chmod +x ~/src/primarykey/test.sh
    ~/src/primarykey/test.sh ${OPERATIONS:=8000}
else
    echo "test.sh script not found."
    exit 1
fi

if [ -n "${POSTGRES_PID}" ]; then
    echo "Stopping PostgreSQL with PID ${POSTGRES_PID}"
    kill ${POSTGRES_PID}
fi
