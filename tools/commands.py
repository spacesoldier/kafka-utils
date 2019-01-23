from common import find_file_path, run_command, check_empty_arg, write_to_file

import StringIO
import pandas as pd


def list_consumer_groups(server_url):
    script_path = find_file_path('kafka-consumer-groups.sh')
    list_cgroup_params = [script_path, '--bootstrap-server', server_url, '--list']
    p_out, p_err = run_command(list_cgroup_params)
    print(p_out)
    return p_out[:-1].split('\n')


def describe_consumer_group(group_name, server_url):
    print('group: {0}'.format(group_name))
    script_path = find_file_path('kafka-consumer-groups.sh')
    describe_cgroup_params = [script_path, '--bootstrap-server', server_url, '--describe', '--group', group_name]
    p_out, p_err = run_command(describe_cgroup_params)
    return p_out[:-1]


def list_group_topics(description_str):
    descr = StringIO.StringIO(description_str)
    descr_df = pd.read_table(descr, sep='\s+|\t+', header=0, engine='python')

    print(list(descr_df.TOPIC.unique()))
    return list(descr_df.TOPIC.unique())


# use this function to backup current offsets for the topics before the reset offsets
def save_group_offsets(group_name, path, description_str):
    if path is not None and path != '':
        descr = StringIO.StringIO(description_str)
        descr_df = pd.read_table(descr, sep='\s+|\t+', header=0, engine='python')

        backup_df = descr_df[['TOPIC', 'PARTITION', 'CURRENT-OFFSET']]
        print(backup_df)
        write_to_file(path, backup_df.to_csv(), group_name+'.csv')


# this function will load previous offsets for the given consumer group from the backup
def load_group_offsets(group_name, path):
    #TODO: define the way to calc offset for a topic having various partitions offsets
    pass


def reset_topic_offset(group_name, topic_name, server_url, mode, date_str=''):
    try:
        #kafka-consumer-groups --bootstrap-server <kafkahost:port> --group <group_id> --topic <topic_name> --reset-offsets --to-earliest
        reset_mode = {
            'earliest': ['--to-earliest'],
            'latest': ['--to-latest'],
            'to_date': ['--to-datetime', check_empty_arg(date_str, 'date')],
        }
        if mode in reset_mode.keys():
            print('will reset {0} : {1}'.format(group_name, topic_name))
            script_path = find_file_path('kafka-consumer-groups.sh')
            script_params = [script_path,
                             '--bootstrap-server',
                             server_url,
                             '--reset-offsets',
                             '--group',
                             group_name,
                             ]
            script_params.extend(reset_mode[mode])
            if topic_name == '_all_':
                script_params.append('--all-topics')
            else:
                script_params.extend(['--topic', topic_name])

            print('run command: {0}'.format(' '.join(script_params)))

            p_out, p_err = run_command(script_params)

            print('errors: {0}'.format(p_err))

            if 'Error' not in p_out:
                print(p_out)
                print('then reset')
                # script_params.append('--execute')
                # rp_out, rp_err = run_command(script_params)
                # print(rp_err)
                # print('reset result:')
                # print(rp_out)
                # print('check the reset result:')
                # print(describe_consumer_group(group_name, server_url))
            else:
                print('error occured: {0}'.format(p_out))
        else:
            raise Exception('Error: wrong offset argument: {0}'.format(mode))
    except Exception as e:
        print(e)

