from tools.common import setup_args, check_empty_arg
from tools.commands import describe_consumer_group, list_consumer_groups, list_group_topics, reset_topic_offset, save_group_offsets

KAFKA_URL = 'localhost:9092'

args_conf = [
    {
        "short": "-g",
        "full": "--group",
        "dest": "group",
        "help": "consumer group to describe, '_all_' to describe all consumer groups",
        "metavar": "GROUP"
    },
]


# this function lists the current topic offsets for the given consumer group
def describe_consumer_groups(input_args):
    groups = list_consumer_groups(KAFKA_URL)
    print('found {0} groups'.format(len(groups)))

    if 'group' in input_args.keys():
        try:
            if input_args['group'] in groups:
                print(describe_consumer_group(input_args['group'], KAFKA_URL))
            else:
                if input_args['group'] == '_all_':
                    for group in groups:
                        print(describe_consumer_group(group, KAFKA_URL))

        except Exception as e:
            print(e)


if __name__ == '__main__':
    args = setup_args(args_conf)
    describe_consumer_groups(vars(args))




