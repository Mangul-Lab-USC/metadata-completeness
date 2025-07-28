from ftplib import FTP
import os
import tarfile

def get_xml(accession: str):
    acc_dir = accession[:-3] + 'nnn'
    tgz_filename = accession + '_family.xml.tgz'
    ftp_path = f'/geo/series/{acc_dir}/{accession}/miniml/{tgz_filename}'

    ftp = FTP('ftp.ncbi.nlm.nih.gov')
    ftp.login()

    try:
        with open(tgz_filename, 'wb') as fp:
            ftp.retrbinary(f'RETR {ftp_path}', fp.write)
    except Exception as e:
        ftp.quit()
        raise RuntimeError(f"FTP download failed for {accession}: {e}")

    ftp.quit()

    try:
        with tarfile.open(tgz_filename, mode='r:gz') as tf:
            xml_members = [m for m in tf.getnames() if m.endswith('.xml')]
            if xml_members:
                for member in xml_members:
                    tf.extract(member, path='../raw_data/')
            else:
                raise FileNotFoundError(f"No .xml file found in archive: {tgz_filename}. Members: {tf.getnames()}")
    finally:
        os.remove(tgz_filename)

def main():
    os.makedirs('../raw_data', exist_ok=True)

    with open('accession_list.txt', 'r') as acc_file:
        for accession in acc_file:
            accession = accession.strip()
            if accession:
                try:
                    get_xml(accession)
                except Exception as e:
                    print(f"Error processing {accession}: {e}")

if __name__ == '__main__':
    main()
