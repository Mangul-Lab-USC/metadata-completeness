from ftplib import FTP
import numpy as np
import h5py
import os
import tarfile
import sys
import io
from time import perf_counter

# Threshold for download file size should be < 200 MB 
THRESHOLD = 200*1024*1024

class Timer:
    def __init__(self, activity):
        self.activity = activity

    def __enter__(self):
        self.time = perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        self.time = perf_counter() - self.time
        self.readout = f'Activity {self.activity}: {self.time:.3f} seconds'
        print(self.readout)

class H5Data:
    vstr = h5py.string_dtype(encoding='utf-8')
    
    def __init__(self, filename='accession-records-001.h5', size=100):
        self.size = size
        self.h5 = h5py.File(filename, mode='w')
        self.count = 0
        self.failed_accs = []
    
    def write_xml(self, accession_id: str, xml_content: str):
        print(f'[{self.count+1}/{self.size}] h5 saving {accession_id}', )
        self.h5.create_dataset(f'/accession/{accession_id}', data=xml_content, dtype=H5Data.vstr)
        self.count += 1

    def add_failed_accessions(self, accession_id: str):
        self.failed_accs.append(accession_id)

    def close(self):
        print(f'failed accessions: {len(self.failed_accs)}')
        if len(self.failed_accs) > 0:
            self.h5.create_dataset('/data/failed_accessions', data=self.failed_accs, dtype=H5Data.vstr)
        self.h5.close()


def get_accession_records(accession_list_filename):
    recs = []
    with open(accession_list_filename, 'r') as acc_file:
        for line in acc_file:
            recs.append(line.rstrip())
    return recs

def get_xml(i, accession, ftp, h5data):
    acc_dir = accession[:-3] + 'nnn'
    filename = accession + '_family.xml'
    url = '/geo/series/' + acc_dir + '/' + accession + '/miniml/' + filename + '.tgz'
    xml_file = io.BytesIO()
    try:
        file_size = ftp.size(url)
        print(f'[{i+1}/{h5data.size}] downloading => ftp.ncbi.nlm.nih.gov{url} size={file_size}')
        if file_size > THRESHOLD:
            raise ValueError(f'File size {file_size} exceeds {THRESHOLD} limit')
        ftp.retrbinary('RETR ' + url, xml_file.write)
    except Exception as e:
        print(f'Error occurred while downloading {filename}: {e}')
        h5data.add_failed_accessions(accession)
    else:
        try:
            xml_file.seek(0)
            with tarfile.open(fileobj=xml_file, mode='r:gz') as tf:
                xml_content = tf.extractfile(filename).read()
                h5data.write_xml(accession, xml_content)
        except Exception as e:
            print(f'Error occurred while downloading : {e}')

def iter_batch(accession_list, batch_size):
    for i in range(0, len(accession_list), batch_size):
        j = min(len(accession_list), i + batch_size)
        yield accession_list[i:j]

def download_in_batch(accession_list, h5data, batch_size=70):
    batches = iter_batch(accession_list, batch_size)
    for i, batch in enumerate(batches):
        ftp = FTP('ftp.ncbi.nlm.nih.gov')
        ftp.login()
        for j, accession in enumerate(batch):
            get_xml(i*batch_size+j, accession, ftp, h5data)
        ftp.quit()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python script.py accession_list <output hdf file e.g. xyz.h5')
        sys.exit(1)

    accession_list_file = sys.argv[1]
    output_hdf5_file = sys.argv[2]

    #if not os.path.exists(output_hdf5_file):
    accession_list = get_accession_records(accession_list_file)
    h5data = H5Data(output_hdf5_file, size=len(accession_list))
    with Timer('FTP Download') as tmr:
        download_in_batch(accession_list, h5data)
    h5data.close()
