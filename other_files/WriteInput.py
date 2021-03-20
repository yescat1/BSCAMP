import argparse
from sys import argv
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.pvalue import AsList

parser = argparse.ArgumentParser()
parser.add_argument('--time_series', type=str)
args, beam_args = parser.parse_known_args(argv)


class LabelColumns(beam.DoFn):
    def process(self, element, tseries_as_list):
        res = list(tuple(enumerate(tseries_as_list)))
        return res


class MPAsDict(beam.DoFn):
    def process(self, element):
        result = [
            {'col': element[0], 'time_series': element[1]}
        ]
        return result


class PrintPcollection(beam.DoFn):
    def process(self, element):
        print(element)


with beam.Pipeline(argv=beam_args) as pipeline:
    lines = pipeline | ReadFromText(args.time_series)
    temp = pipeline | beam.Create([1])
    final_series = temp | "attaching column numbers" >> beam.ParDo(LabelColumns(), AsList(lines))

    # Writing to Big Query
    time_series_dict = final_series | "Converting to Dict" >> beam.ParDo(MPAsDict())
    table_spec = "scalable_mp.input"
    table_schema = {
        'fields': [{
            'name': 'col', 'type': 'INT64', 'mode': 'NULLABLE'
        }, {
            'name': 'time_series', 'type': 'FLOAT64', 'mode': 'NULLABLE'
        }]
    }

    time_series_dict | "Write to Big Query" >> beam.io.WriteToBigQuery(
        table_spec,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    # Debug function
    debug = final_series | "print " >> beam.ParDo(PrintPcollection())


# python other_files/writeinput.py --time_series
# /Users/kalyesh/PycharmProjects/beam/other_files/SampleInput/randomwalk512K.txt --runner direct --project bscamp11
# --temp_location gs://full_bucket11/temp/ --region us-west1 --save_main_session True > other_files/debug.txt
