#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=0-00:05:00
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