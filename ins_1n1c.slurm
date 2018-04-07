#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --time=0-00:05:00
#SBATCH --partition=physical
#SBATCH -o 1n1c-%j

DATADIR=/home/sxia2/data/output


# Load required modules
module load Python/3.5.2-goolf-2015a


# Launch multiple process python code
echo "Instagram location calculation and sorting with 1n1c"
time mpiexec -n 1 python3 ins_cal.py -i bigInstagram.json -g melbGrid.json > $DATADIR/1n1c-$SLURM_JOB_ID.out
