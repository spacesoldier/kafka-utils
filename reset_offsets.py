from tools.common import setup_args, check_empty_arg
from tools.commands import describe_consumer_group, list_consumer_groups, list_group_topics, reset_topic_offset, save_group_offsets

KAFKA_URL = 'localhost:9092'

args_conf = [
    {
        "short": "-t",
        "full": "--topic",
        "dest": "topic",
        "help": "topic to reset offset, '_all_' to reset all topics",
        "metavar": "TOPIC"
    },

    {
        "short": "-o",
        "full": "--offset",
        "dest": "offset",
        "help": "new offset to reset ('earliest', 'latest', 'to_date')",
        "metavar": "OFFSET"
    },

    {
        "short": "-d",
        "full": "--date",
        "dest": "date",
        "help": "date to reset to (Format: 'YYYY-MM-DDTHH:mm:SS.sss')",
        "metavar": "DATE"
    },

    {
        "short": "-b",
        "full": "--backup-to",
        "dest": "offsets_path",
        "help": "path to directory where the current offsets will be stored",
        "metavar": "OFFSETS_PATH"
    }
]


# this function finds the given topic in all consumer groups and changes its current offset to the given value or date
def reset_offsets_for_topic_in_all_groups(input_args):
    groups = list_consumer_groups(KAFKA_URL)
    print('found {0} groups'.format(len(groups)))

    try:
        # all(x in ['b', 'a', 'foo', 'bar'] for x in ['a', 'b']) <-- check if all elements ('a','b') are in list ('b','a','foo','bar')
        if all(x in input_args.keys() for x in ['topic', 'offset']):
            reset_topic = input_args['topic']
            print('will reset topic: {0}'.format(check_empty_arg(reset_topic,'topic')))

            if 'offset' == 'to_date':
                if 'date' in input_args.keys():
                    reset_date = check_empty_arg(input_args['date'], 'date')
                else:
                    reset_date = ''

            logs_structure = {}

            for group in groups:
                about = describe_consumer_group(group, KAFKA_URL)

                if 'offsets_path' in input_args.keys():
                    offsets_path = input_args['offsets_path']
                    save_group_offsets(group, offsets_path, about)

                logs_structure[group] = list_group_topics(about)
                if reset_topic == '_all_' or reset_topic in logs_structure[group]:
                    reset_topic_offset(group, reset_topic, KAFKA_URL, input_args['offset'], reset_date)
    except Exception as e:
        print(e)


if __name__=='__main__':
    args = setup_args(args_conf)
    reset_offsets_for_topic_in_all_groups(vars(args))

