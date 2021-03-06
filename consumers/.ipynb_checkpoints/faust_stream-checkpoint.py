"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("connect-stations", value_type=Station)
out_topic = app.topic("stations.faust.table", partitions=1)
table = app.Table(
    "stations.faust.table",
    default= TransformedStation,
    partitions=1,
    changelog_topic= out_topic    
)
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
# topic = app.topic("TODO", value_type=Station)
# TODO: Define the output Kafka Topic
# out_topic = app.topic("TODO", partitions=1)
# TODO: Define a Faust Table
#table = app.Table(
#    # "TODO",
#    # default=TODO,
#    partitions=1,
#    changelog_topic=out_topic,
#)

@app.agent(topic)
async def stations_reorg(stations):
    async for station in stations:
        if station.red == True:
            line = "red"
        elif station.blue == True:
            line = "blue"
        elif station.green == True:
            line = "green"
        else:
            line = "Null"
        print('station_reorg:', station.station_id)
        table[station.station_id] = TransformedStation(
            station_id = station.station_id,
            station_name = station.station_name,
            order = station.order,
            line = line
        )
        print(f"{station.station_id}: {line}")
            
    

#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#


if __name__ == "__main__":
    app.main()
