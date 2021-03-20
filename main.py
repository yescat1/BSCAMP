from sys import argv
import argparse
import itertools
import numpy as np
import pyscamp
import logging
import time
import apache_beam as beam
from apache_beam.pvalue import AsList
from datetime import timedelta
from google.cloud import storage, bigquery
from apache_beam.options.pipeline_options import PipelineOptions

start = time.time()
parser = argparse.ArgumentParser()
parser.add_argument('--time_series', type=str)
parser.add_argument('--subsequence_length', type=int)
parser.add_argument('--tile_size', type=int)
args, beam_args = parser.parse_known_args(argv)


def rawcount(filename):
    f = open(filename, 'rb')
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.raw.read

    buf = read_f(buf_size)
    while buf:
        lines += buf.count(b'\n')
        buf = read_f(buf_size)

    return lines


class segment_boundaries(beam.DoFn):
    def process(self, element, num_of_segments, time_series_length):
        res1 = (element * args.tile_size)
        if element != num_of_segments - 1:
            res2 = ((element + 1) * args.tile_size) + args.subsequence_length - 1
            res = [[res1, res2]]
            return res
        else:
            res2 = time_series_length
            res = [[res1, res2]]
            return res


class tile_pairs(beam.DoFn):
    def process(self, element, indexes, intervals):
        keys, values = [], []
        for i, j in itertools.product(indexes, indexes):
            keys.append((i, j))
        for pairX, pairY in itertools.product(intervals, intervals):
            values.append((pairX, pairY))
        result = list(zip(keys, values))
        return result


class SCAMP(beam.DoFn):
    def process(self, element, time_series):
        time_series = np.asarray(time_series, dtype=np.float64)
        if element[0][0] != element[0][1]:
            profile, index = pyscamp.abjoin(time_series[element[1][0][0]:element[1][0][1]],
                                            time_series[element[1][1][0]:element[1][1][1]],
                                            args.subsequence_length)
        else:
            profile, index = pyscamp.selfjoin(time_series[element[1][0][0]:element[1][0][1]],
                                              args.subsequence_length)

        profile = profile.tolist()
        index = (index + element[1][1][0]).tolist()
        mp_triplet = list(tuple(enumerate(zip(profile, index), element[1][0][0])))
        return mp_triplet

# res = tuple(enumerate(zip(profile, index), element[1][0][0])
# res = [(col, row(mpi), mp value), ( ), ....]

# [col, row(mpi), mp value]


class ElementWiseMin(beam.DoFn):
    def process(self, element):
        min1 = min(map(lambda x: x[1], element[1]))
        res = [(element[0], min1)]
        return res


class MPAsDict(beam.DoFn):
    def process(self, element):
        result = [
            {'column': element[0], 'mpi': element[1][1], 'mp': element[1][0]}
        ]
        return result


class PrintPcollection(beam.DoFn):
    def process(self, element):
        print(element)


with beam.Pipeline(argv=beam_args) as pipeline:
    time_series_length = rawcount(args.time_series)
    num_of_segments = time_series_length // args.tile_size

    # reading the time_series from BigQuery
    input_pcoll = pipeline | 'QueryTableStdSQL' >> beam.io.ReadFromBigQuery(query="SELECT col, time_series FROM bscamp111.scalable_mp.input ORDER BY col", use_standard_sql=True)
    time_series = input_pcoll | beam.Map(lambda elem: elem['time_series'])

    # Collection of segment boundaries
    tile_numbers = pipeline | "Tile numbers" >> beam.Create([i for i in range(0, num_of_segments)])
    segments = tile_numbers | "Segment boundaries" >> beam.ParDo(segment_boundaries(), num_of_segments, time_series_length)
    temp = pipeline | "Tiles - 1" >> beam.Create([1])

    # collection of tiles
    tiles = temp | "Tiles - 2" >> beam.ParDo(tile_pairs(), AsList(tile_numbers), AsList(segments))

    # Partial Outputs
    output = tiles | "Partial Outputs" >> beam.ParDo(SCAMP(), AsList(time_series))

    # Grouping Partial Outputs by tile column
    group = output | "Grouping Partial outputs" >> beam.GroupBy(lambda s: s[0])

    # Merging Outputs
    element_wise_min = group | "Merging partial outputs" >> beam.ParDo(ElementWiseMin())

    # Writing to Big Query
    matrix_profile = element_wise_min | "Converting to Dict" >> beam.ParDo(MPAsDict())

    table_spec = "scalable_mp.bscamp1"
    table_schema = {
        'fields': [{
            'name': 'column', 'type': 'INT64', 'mode': 'NULLABLE'
        }, {
            'name': 'mpi', 'type': 'INT64', 'mode': 'NULLABLE'
        }, {
            'name': 'mp', 'type': 'FLOAT64', 'mode': 'NULLABLE'
        }]
    }

    # matrix_profile | "Write to Big Query" >> beam.io.WriteToBigQuery(
    #     table_spec,
    #     schema=table_schema,
    #     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    # Debug function
    # debug = matrix_profile | "print " >> beam.ParDo(PrintPcollection())

end = time.time()
print(str(timedelta(seconds=(end-start))))