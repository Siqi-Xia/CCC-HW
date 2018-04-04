from mpi4py import MPI
import json
import getopt, sys

MASTER_RANK = 0


def pro_grid(inputfile):
    grid_list = []
    data_dict = {}
    data_list = json.load(open(inputfile))
    for i in range(len(data_list['features'])):
        grid_list.append(data_list['features'][i]['properties'])
        data_dict[data_list['features'][i]['properties']['id']] = 0
    return grid_list, data_dict

def pro_cell_position(cell_dict,grid_ls,data_dict):
    if 'coordinates' in cell_dict['doc']:
        cell = cell_dict['doc']['coordinates']['coordinates']
        for i in range(len(grid_ls)):
            if cell[0]!= None and cell[1] != None:
                if cell[0] > grid_ls[i]['ymin'] and cell[0] <= grid_ls[i]['ymax'] and cell[1] > grid_ls[i]['xmin'] and cell[1] <= grid_ls[i]['xmax']:
                    data_dict[grid_ls[i]['id']] += 1
            else:
                data_dict[grid_ls[i]['id']] += 1
    return data_dict


def sort_dict(data_dict):
    return sorted(data_dict.items() , key=lambda t : t[1],reverse=True)

def get_row(cell):
    row_dict = {}
    row_dict['A-Row'] = 0
    row_dict['B-Row'] = 0
    row_dict['C-Row'] = 0
    row_dict['D-Row'] = 0
    for keys,values in cell.items():
        if 'A' in keys:
            row_dict['A-Row'] += values
        if 'B' in keys:
            row_dict['B-Row'] += values
        if 'C' in keys:
            row_dict['C-Row'] += values
        if 'D' in keys:
            row_dict['D-Row'] += values
    return sort_dict(row_dict)


def get_column(cell):
    col_dict = {}
    col_dict['Column 1'] = 0
    col_dict['Column 2'] = 0
    col_dict['Column 3'] = 0
    col_dict['Column 4'] = 0
    col_dict['Column 5'] = 0
    for keys,values in cell.items():
        if '1' in keys:
            col_dict['Column 1'] += values
        if '2' in keys:
            col_dict['Column 2'] += values
        if '3' in keys:
            col_dict['Column 3'] += values
        if '4' in keys:
            col_dict['Column 4'] += values
        if '5' in keys:
            col_dict['Column 5'] += values
    return sort_dict(col_dict)

def load_json(input):
    data_list = json.loads(input)
    return data_list


def DelLastChar(str):
    str_list=list(str)
    a = str_list.pop()
    if a != ',':
        str_list.append(a)
    return "".join(str_list)

def load_ins_json(line):
    line = line.rstrip()
    line = DelLastChar(line)
    data_line = load_json(line)
    return data_line



def read_arguments(argv):
    # Initialise Variables
    inputfile1 = ''
    inputfile2 = ''
    ## Try to read in arguments
    try:
        opts, args = getopt.getopt(argv,"hi:g:")
    except getopt.GetoptError as error:
        print(error)
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
          print_usage()
          sys.exit()
        elif opt in ("-i"):
           inputfile = arg
        elif opt in ("-g"):
           inputfile2 = arg

    # Return all the arguments
    return inputfile, inputfile2

def process_ins(rank, input_file, processes, grid_list, data_dict):
    i = 0
    with open(input_file) as f:
        line = f.readline()
        # Send tweets to slave processes
        while line:
            if i%processes == rank:
                try:
                    cell_dict = load_ins_json(line)
                    cell_pos = pro_cell_position(cell_dict, grid_list, data_dict)
                except ValueError:
                    print("Malformed JSON in ins ")
            line = f.readline()
            i += 1
    return cell_pos


def marshall_ins(comm):
    processes = comm.Get_size()
    counts = [] 
    #Now ask all processes except oursevles to return counts
    for i in range(processes-1):
        # Send request
        comm.send('return_data', dest=(i+1), tag=(i+1))
    for i in range(processes-1):
        # Receive data
        counts.append(comm.recv(source=(i+1), tag=MASTER_RANK))
    return counts

def master_ins_processor(comm, input_file, grad_list, data_dict):
    # Read our tweets
    rank = comm.Get_rank()
    size = comm.Get_size()

    cell_pos = process_ins(rank, input_file, size, grad_list, data_dict)
    if size > 1:
        counts = marshall_ins(comm)
        # Marshall that data
        for d in counts:
            for k,v in d.items():
                cell_pos[k] = cell_pos.setdefault(k,0) + v
        # Turn everything off
        for i in range(size-1):
            # Receive data
            comm.send('exit', dest=(i+1), tag=(i+1))

    # Print output

    print('Sorted areas:\n',sort_dict(cell_pos))
    print('Sorted rows:\n', get_row(cell_pos))
    print('Sorted columns:\n', get_column(cell_pos))


def slave_ins_processor(comm,input_file, grad_list, data_dict):
  # We want to process all relevant tweets and send our counts back
  # to master when asked
  # Find my tweets
    rank = comm.Get_rank()
    size = comm.Get_size()

    counts = process_ins(rank, input_file, size, grad_list, data_dict)
    # Now that we have our counts then wait to see when we return them.
    while True:
        in_comm = comm.recv(source=MASTER_RANK, tag=rank)
        # Check if command
        if isinstance(in_comm, str):
            if in_comm in ("return_data"):
                # Send data back
                # print("Process: ", rank, " sending back ", len(counts), " items")
                comm.send(counts, dest=MASTER_RANK, tag=MASTER_RANK)
            elif in_comm in ("exit"):
                exit(0)

def main(argv):
    # Get

    input_file, inputfile2 = read_arguments(argv)
    grid_list, data_dict = pro_grid(inputfile2)

    # Work out our rank, and run either master or slave process
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if rank == 0 :
        # We are master
        master_ins_processor(comm,input_file, grid_list, data_dict)
    else:
        # We are slave
        slave_ins_processor(comm,input_file, grid_list, data_dict)

# Run the actual program
if __name__ == "__main__":
    main(sys.argv[1:])
