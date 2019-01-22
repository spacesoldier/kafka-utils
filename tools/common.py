import os
from subprocess import Popen, PIPE
from argparse import ArgumentParser


def run_command(list_cgroup_params):
    process = Popen(list_cgroup_params, stdout=PIPE, stderr=PIPE)
    p_out, p_err = process.communicate()
    return p_out, p_err


def find_file_path(file_name):
    find_params = ['find', '/', '-type', 'f', '-name', file_name]
    p_out = run_command(find_params)
    return p_out[:-1]


def parse_args(args_list):
    parser = ArgumentParser()
    for arg in args_list:
        parser.add_argument(arg['short'], arg['full'], dest=arg['dest'], help=arg['help'], matavar=arg['metavar'])

    return parser.parse_args()


# this function helps stop the mission-critical task if the given empty arg will cause a problem
def check_empty_arg(arg, arg_name):
    if arg == '':
        raise Exception('empty argument: {0}'.format(arg_name))
    else:
        return arg


# this function helps to dump data to file (use to backup offsets for example)
def write_to_file(dir_path, fdata, filename, mode='replace'):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    with open(dir_path+'/'+filename, 'w+') as file:
        if mode == 'replace':
            file.seek(0)
        file.write(fdata)
        if mode == 'replace':
            file.truncate()


