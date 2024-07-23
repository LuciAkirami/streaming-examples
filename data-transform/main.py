import os
from quixstreams import Application
import uuid
import json

# for local dev, load env vars from a .env file
from dotenv import load_dotenv

load_dotenv()

# randomly generating uuid, so everytime the app is called, the transformations will start from the
# beginning. Only used during development
# app = Application(consumer_group=str(uuid.uuid4()), auto_offset_reset="earliest")
app = Application(consumer_group="data-transformation-v1", auto_offset_reset="earliest")

input_topic = app.topic(os.environ["input"])
output_topic = app.topic(os.environ["output"])

sdf = app.dataframe(input_topic)

# put transformation logic here
# see docs for what you can do
# https://quix.io/docs/get-started/quixtour/process-threshold.html

# we need only the payload and not the metada
# if True, expand the returned iterable into individual values downstream
# This is because, each message contains multiple records / entries, i.e. array of msgs and we want
# to expand them to individual items
sdf = sdf.apply(lambda msg: msg["payload"], expand=True)

# let us do some more transformations


def transform(row: dict) -> dict:
    new_row = {}
    new_row["time"] = row["time"]

    for key in row["values"]:
        new_row[row["name"] + "-" + key] = row["values"][key]

    return new_row


sdf = sdf.apply(transform)

# we are interested in only the dictionaries that contain the 'accelerometer-x'
# you can check this by running it with commenting it and without commenting it
sdf = sdf[sdf.contains("accelerometer-x")]

# create a new column
sdf["accelerometer-total"] = (
    sdf["accelerometer-x"].abs()
    + sdf["accelerometer-y"].abs()
    + sdf["accelerometer-z"].abs()
)
# now our dict will contain exactly 5 keys, that is 3 accelerometer readings, 1 total and 1 time

# useful when debugging
# sdf = sdf.update(lambda row: print(json.dumps(row, indent=4)))

# update() function:
# Apply a function to mutate value in-place or to perform a side effect that doesn't update the value (e.g. print a value to the console).
sdf = sdf.update(lambda row: print(list(row.values())))

# now we publish the transformed data to a topic
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)
