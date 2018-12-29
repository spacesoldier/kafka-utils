import StringIO
from subprocess import Popen, PIPE
import pandas as pd

from argparse import ArgumentParser

KAFKA_URL = 'localhost:9092'

def find_file_path(file_name):
    process = Popen(['find', '/', '-type', 'f', '-name', file_name], stdout=PIPE, stderr=PIPE)
    p_out, p_err = process.communicate()
    return p_out[:-1]


def list_consumer_groups(server_url):
    script_path = find_file_path('kafka-consumer-groups.sh')
    process = Popen([script_path, '--bootstrap-server', server_url, '--list'], stdout=PIPE, stderr=PIPE)
    p_out, p_err = process.communicate()
    print(p_out)
    return p_out[:-1].split('\n')


def describe_consumer_group(group_name, server_url):
    print('group: {0}'.format(group_name))
    script_path = find_file_path('kafka-consumer-groups.sh')
    process = Popen([script_path, '--bootstrap-server', server_url, '--describe', '--group', group_name], stdout=PIPE, stderr=PIPE)
    p_out, p_err = process.communicate()
    return p_out[:-1]


def list_group_topics(description_str):
    descr = StringIO.StringIO(description_str)
    descr_df = pd.read_table(descr, sep='\s+|\t+', header=0, engine='python')

    print(list(descr_df.TOPIC.unique()))
    return list(descr_df.TOPIC.unique())


def reset_topic_offset(group_name, topic_name, server_url, mode):
    #kafka-consumer-groups --bootstrap-server <kafkahost:port> --group <group_id> --topic <topic_name> --reset-offsets --to-earliest
    reset_mode = {
        'earliest': '--to-earliest',
        'latest': '--to-latest'
    }
    if mode in reset_mode.keys():
        print('will reset {0} : {1}'.format(group_name, topic_name))
        script_path = find_file_path('kafka-consumer-groups.sh')
        process = Popen([script_path, '--bootstrap-server', server_url, '--reset-offsets', '--group', group_name, '--topic', topic_name, reset_mode[mode]],
                        stdout=PIPE,
                        stderr=PIPE)
        p_out, p_err = process.communicate()
        print('errors: {0}'.format(p_err))
        if 'Error' not in p_out:
            print(p_out)
            print('then reset')
            reset_process = Popen(
                [script_path, '--bootstrap-server', server_url, '--reset-offsets', '--group', group_name, '--topic',
                 topic_name, reset_mode[mode], '--execute'],
                stdout=PIPE,
                stderr=PIPE)
            rp_out, rp_err = reset_process.communicate()
            print(rp_err)
            print('reset result:')
            print(rp_out)
            print('check the reset result:')
            print(describe_consumer_group(group_name, server_url))
        else:
            print('consumers running')
    else:
        print('Error: wrong offset argument: {0}'.format(mode))


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("-t", "--topic", dest="topic",
                        help="topic to reset offset", metavar="TOPIC")

    parser.add_argument("-o", "--offset", dest="offset",
                        help="new offset to reset ('earliest', 'latest')", metavar="OFFSET")

    return parser.parse_args()


def main(input):
    groups = list_consumer_groups(KAFKA_URL)
    print('found {0} groups'.format(len(groups)))

    # all(x in ['b', 'a', 'foo', 'bar'] for x in ['a', 'b']) <-- check if all elements ('a','b') are in list ('b','a','foo','bar')
    if all(x in input.keys() for x in ['topic', 'offset']):
        reset_topic = input['topic']
        print('will reset topic: {0}'.format(reset_topic))
        logs_structure = {}

        for group in groups:
            about = describe_consumer_group(group, KAFKA_URL)
            logs_structure[group] = list_group_topics(about)
            if reset_topic in logs_structure[group]:
                reset_topic_offset(group, reset_topic, KAFKA_URL, input['offset'])


if __name__=='__main__':
    args = parse_args()
    main(vars(args))

