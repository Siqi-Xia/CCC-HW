#!/bin/bash
# 2 nodes, 2 tasks per node = 4 cores
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=2
#SBATCH --time=0-12:00:00
#SBATCH --partition=physical

# Load required modules
module load Python/3.5.2-goolf-2015a

# Launch multiple process python code
echo "Instagram in cell sorting"
time mpiexec -n 8 python3 ins_cal.py -i mediumInstagram.json -g melbGrid.json -p
echo "Instagram in row sorting"
time mpiexec -n 8 python3 ins_cal.py -i mediumInstagram.json -g melbGrid.json -r
echo "Instagram in column sorting"
time mpiexec -n 8 python3 ins_cal.py -i mediumInstagram.json -g melbGrid.json -c