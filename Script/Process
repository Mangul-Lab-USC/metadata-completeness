import pandas
import pprint
import h5py
import argparse
import sys
import pandas as pd
from xml_parser import parse_xml

class H5DataReader:
    def __init__(self, filename: str):
        self.h5 = h5py.File(filename, 'r')

    def read_xml(self, accession_id: str):
        xml_content = self.h5[f'/accession/{accession_id}'][()]
        return xml_content.decode('utf-8')

    def parse_xml(self, accession_id: str):
        print(f'parsing {accession_id}')
        xml_content = h5data.read_xml(accession_id)
        data = parse_xml(accession_id, xml_content)
        return data

    def extract_summary_table(self):
        accession_list = self.h5['/accession'].keys()
        table = []
        for accession_id in accession_list:
            try:
                table += self.parse_xml(accession_id)
            except Exception as e:
                print(f'error in processing {accession_id}')
        return table

    def close(self):
        self.h5.close()


def export_csv(table, filename='data.csv'):
    df = pd.DataFrame(table)
    df.to_csv(filename, index=False)
    #print(f'written {len(table)} records to {filename}')

class AccessionDataFrame:
    def __init__(self):
        self.df = pd.DataFrame()

    def add_data(self, accession_id, xml_content):
        data = parse_xml(accession_id, xml_content)
        df_data = pd.DataFrame.from_dict(data)
        self.df = pd.concat([self.df, df_data], ignore_index=True, sort=False)

    def close(self):
        self.df.to_csv('summary_data.csv', index=False)

#def get_parser():
#
#    # Create the parser
#    parser = argparse.ArgumentParser(description="Accession Options")
#
#
#    # Add the mandatory argument
#    parser.add_argument('h5_file', type=str, help='HDF5 File')
#    parser.add_argument('accession_id', type=str, help='Accession ID')
#
#
#    # Create a mutually exclusive group for extract and parse options
#    group = parser.add_mutually_exclusive_group()
#
#    # Add the command-line options to the mutually exclusive group
#    group.add_argument('-e', '--extract-accession', action='store_true', help='Extract accession')
#    group.add_argument('-p', '--parse-accession', action='store_true', help='Parse accession')
#    group.add_argument('-p', '--parse-accession', action='store_true', help='Parse accession')
#
#    return parser

def get_parser():
    # Create the parser
    parser = argparse.ArgumentParser(description="Accession Options")

    # Add the arguments
    parser.add_argument('h5_file', type=str, help='HDF5 File')

    # Create a mutually exclusive group for extract and parse options
    group = parser.add_mutually_exclusive_group()

    # Add the command-line options to the mutually exclusive group
    group.add_argument('-e', '--extract-accession', metavar='accession_id', type=str, help='Extract accession by ID')
    group.add_argument('-p', '--parse-accession', metavar='accession_id', type=str, help='Parse accession by ID')
    parser.add_argument('-c', '--convert-csv', metavar='csv_file', type=str,  dest='csv_file', help='Convert to CSV')

    return parser



if __name__ == '__main__':
    # Parse the command-line arguments
    parser = get_parser()
    args = parser.parse_args()

    h5data = H5DataReader(args.h5_file)

    # Process the selected options
    if args.extract_accession:
        xml_content = h5data.read_xml(args.extract_accession)
        print(xml_content)
    elif args.parse_accession:
        data = h5data.parse_xml(args.parse_accession)
        pprint.pprint(data)
    elif args.csv_file:
        table = h5data.extract_summary_table()
        export_csv(table, args.csv_file)
    else:
        parser.print_help()

    h5data.close()
