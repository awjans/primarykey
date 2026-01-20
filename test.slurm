#!/usr/bin/env bash
#SBATCH --job-name=primarykey_test
#SBATCH --output=primarykey_test.out
#SBATCH --error=primarykey_test.err
#SBATCH --time=00:00:10
#SBATCH --ntasks=2
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G

export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=password

if [ "${HOSTNAME}" != "ArchChancellor" ]; then
    module load singularity
fi

mkdir -p /tmp/pgdata /tmp/pgrun

singularity pull --disable-cache --dir /tmp --force postgres.sif docker://postgres:18.1

singularity run -B /tmp/pgdata:/var/lib/postgresql/18 -B /tmp/pgrun:/var/run/postgresql /tmp/postgres.sif &
export POSTGRES_PID=${$}
if [ -z "${POSTGRES_PID}" ]; then
    echo "Failed to start PostgreSQL in Singularity container."
    exit 1
fi
if ! pg_isready -h localhost -p 5432 -U postgres > /dev/null 2>&1; then
    echo "PostgreSQL is not ready. Exiting."
    kill ${POSTGRES_PID}
    exit 1
fi

if [ "${HOSTNAME}" != "ArchChancellor" ]; then
    export OPERATIONS=16000000
fi

~/src/primarykey/test.sh ${OPERATIONS:=16000}

kill ${POSTGRES_PID}