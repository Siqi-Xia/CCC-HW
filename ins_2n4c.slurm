#!/bin/bash
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=0-00:05:00
#SBATCH --partition=physical
#SBATCH -o 2n4c-%j

DATADIR=/home/sxia2/data/output


# Load required modules
module load Python/3.5.2-goolf-2015a


# Launch multiple process python code
echo "Instagram location calculation and sorting"
time mpiexec -n 8 python3 ins_cal.py -i bigInstagram.json -g melbGrid.json > $DATADIR/2n4c-$SLURM_JOB_ID.out
