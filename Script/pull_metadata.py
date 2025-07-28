import pandas as pd
import xmltodict
import os

def parse_xml(filepath):
    with open(filepath, encoding='utf-8', errors='replace') as fd:
        data_dict = xmltodict.parse(fd.read())

    accession = filepath.split('/')[-1].split('_')[0]
    print(accession + '\n')
    rows = []

    for sample in data_dict['MINiML']['Sample']:
        tmp = {}

        if 'Status' in sample:
            tmp['database'] = sample['Status'].get('@database', '')
            tmp['submission_date'] = sample['Status'].get('Submission-Date', '')
            tmp['release_date'] = sample['Status'].get('Release-Date', '')
            tmp['last_update_date'] = sample['Status'].get('Last-Update-Date', '')

        tmp['title'] = sample.get('Title', '')
        tmp['accession'] = sample['Accession'].get('#text', sample['Accession'])
        tmp['type'] = sample.get('Type', '')

        channel = sample.get('Channel', {})
        if isinstance(channel, list):
            channel = channel[0]

        tmp['source'] = channel.get('Source', '')
        organism = channel.get('Organism', '')
        tmp['organism'] = organism.get('#text', organism) if isinstance(organism, dict) else organism

        characs = channel.get('Characteristics', [])
        if isinstance(characs, dict):
            characs = [characs]
        for i, c in enumerate(characs):
            if isinstance(c, dict):
                tag = c.get('@tag', f'characteristic_{i}')
                value = c.get('#text', '')
            else:
                tag, value = f'characteristic_{i}', c
            tmp[tag] = value

        tmp['treatment_protocol'] = channel.get('Treatment-Protocol', '')
        tmp['molecule'] = channel.get('Molecule', '')
        tmp['extract_protocol'] = channel.get('Extract-Protocol', '')
        tmp['label'] = channel.get('Label', '')
        tmp['label_protocol'] = channel.get('Label-Protocol', '')
        tmp['hybridization_protocol'] = sample.get('Hybridization-Protocol', '')
        tmp['scan_protocol'] = sample.get('Scan-Protocol', '')
        tmp['description'] = sample.get('Description', '')
        tmp['data_processing'] = sample.get('Data-Processing', '')

        rows.append(tmp)

    df = pd.DataFrame(rows)
    os.makedirs('../summary_data', exist_ok=True)
    df.to_csv(f'../summary_data/{accession}_metadata.csv', index=False)

def combine_csvs(directory='../summary_data', key_column='accession'):
    combined_df = pd.DataFrame()

    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            df = pd.read_csv(filepath)
            if key_column in df.columns:
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            else:
                print(f"Skipping {filename}: missing '{key_column}' column")

    combined_df.to_csv(os.path.join(directory, 'combined_metadata.csv'), index=False)
    print(f"Combined {len(combined_df)} rows into 'combined_metadata.csv'")

if __name__ == '__main__':
    for file in os.listdir('../raw_data'):
        if file.endswith('.xml'):
            parse_xml(os.path.join('../raw_data', file))
    combine_csvs()
