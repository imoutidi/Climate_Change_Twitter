import pickle
import csv
import time
import datetime
from datetime import date, timedelta

def datetify(date_str):
    if "-" in date_str:
        split_date = date_str.split("-")
        if split_date[0] == "nan":
            result = "nan"
        else:
            result = date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
    elif "/" in date_str:
        split_date = date_str.split("/")
        if split_date[0] == "nan":
            result = "nan"
        else:
            result = date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
    else:
        print("Other delimiter in date")
        print(date_str)
        result = "nan"

    return result


def iso_year_start(iso_year):
    # The Gregorian calendar date of the first day of the given ISO year
    fourth_jan = date(iso_year, 1, 4)
    delta = timedelta(fourth_jan.isoweekday()-1)
    return fourth_jan - delta


def iso_to_gregorian(iso_year, iso_week, iso_day):
    # The Gregorian calendar date for the given ISO year, week and day
    year_start = iso_year_start(iso_year)
    return year_start + timedelta(days=iso_day-1, weeks=iso_week-1)


# Converts a string date time (2020-12-28 01:58:26) into an int timestamp (1609120706)
def str_datetime_to_timestamp(date_str):
    # convert date time string into an object
    date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(date_obj.timetuple()))
    return timestamp


def timestamp_to_datetime_obj(timestamp):
    # Convert the timestamp to a datetime object in the local timezone
    datetime_obj = datetime.datetime.fromtimestamp(timestamp)
    return datetime_obj


def line_count(file_path):
    counter = 1
    with open(file_path) as node_file:
        node_reader = csv.reader(node_file, delimiter=",")
        # Skipping the headers
        next(node_reader)
        for node_info in node_reader:
            counter += 1
        print("Number of lines: " + str(counter))
    return counter


def save_pickle(path, data_structure):
    save_ds = open(path, "wb")
    pickle.dump(data_structure, save_ds)
    save_ds.close()


def load_pickle(path):
    load_ds = open(path, "rb")
    data_structure = pickle.load(load_ds)
    load_ds.close()
    return data_structure


def divide_list_into_chunks(lst, num_chunks):
    if num_chunks <= 0:
        raise ValueError("Number of chunks must be greater than 0")
    chunk_size = len(lst) // num_chunks
    remainder = len(lst) % num_chunks
    chunks = []
    start = 0
    for _ in range(num_chunks):
        if remainder > 0:
            end = start + chunk_size + 1
            remainder -= 1
        else:
            end = start + chunk_size
        chunks.append(lst[start:end])
        start = end
    return chunks


if __name__ == "__main__":
    for i in range(1, 51):
        c_date = iso_to_gregorian(2016, i, 1)
        print(str(c_date.year) + " " + str(c_date.month) + " " + str(c_date.day))
