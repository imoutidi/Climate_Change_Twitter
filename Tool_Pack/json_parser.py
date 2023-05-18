import json


def parse_json():
    with open(r'../../../Downloads/Climate Change Twitter Dataset/littman_tweets_0000.json', encoding='utf8') \
            as json_file:
        for line in json_file:
            data = json.loads(line)
            print(data["full_text"])


if __name__ == "__main__":
    parse_json()