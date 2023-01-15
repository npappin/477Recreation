#!/usr/bin/env python3

import requests, json, os, shutil
from tqdm import tqdm
from retry import retry
from retry.api import retry_call
import pandas as pd
from datetime import datetime

@retry(requests.exceptions.ChunkedEncodingError, tries=5, delay=2, backoff=5)
def downloadWrapper(session, url):
    r = session.get(url)
    return r

def processData(df: pd.DataFrame) -> dict:
    # Create state level dataFrames
    dfBslBlock = df
    dfBslH3 = df
    dfBlock = df
    dfH3 = df
    
    # Create block to number of bsl lookup table
    BslBlockDropList = [
        'frn',
        'provider_id',
        'brand_name',
        'technology',
        'max_advertised_download_speed',
        'max_advertised_upload_speed',
        'low_latency',
        'business_residential_code',
        'state_usps',
        'h3_res8_id'
    ]
    dfBslBlock = dfBslBlock.drop(columns=BslBlockDropList)
    dfBslBlock = dfBslBlock.drop_duplicates(subset='location_id')
    dfBslBlock = dfBslBlock.drop(columns='location_id')
    dfBslBlock = dfBslBlock.groupby(dfBslBlock.columns.tolist()).size().reset_index().rename(columns={0:'bsls'})
    
    # Create H3 to number of bsl lookup table
    BslH3DropList = [
        'frn',
        'provider_id',
        'brand_name',
        'technology',
        'max_advertised_download_speed',
        'max_advertised_upload_speed',
        'low_latency',
        'business_residential_code',
        'state_usps',
        'block_geoid'
    ]
    dfBslH3 = dfBslH3.drop(columns=BslH3DropList)
    dfBslH3 = dfBslH3.drop_duplicates(subset='location_id')
    dfBslH3 = dfBslH3.drop(columns='location_id')
    dfBslH3 = dfBslH3.groupby(dfBslH3.columns.tolist()).size().reset_index().rename(columns={0:'bsls'})
    
    # Summarize counts of offers aggregated to the block 
    dfBlock = dfBlock.drop(columns=['location_id', 'h3_res8_id'])
    dfBlock = dfBlock.groupby(dfBlock.columns.tolist()).size().reset_index().rename(columns={0:'bslsOffered'})
    
    # Summarize counts of offers aggregated to the h3 tile
    dfH3 = dfH3.drop(columns=['location_id', 'block_geoid'])
    dfH3 = dfH3.groupby(dfH3.columns.tolist()).size().reset_index().rename(columns={0:'bslsOffered'})
    
    data = {
        'block': dfBlock,
        'h3_8': dfH3
    }
    
    # return two dataframes in a dictionary
    return data

def download():
    s = requests.Session()
    s.headers.update({'User-Agent': 'ArcGIS'})
    response = s.get("https://broadbandmap.fcc.gov/nbm/map/api/published/filing")
    parsed = json.loads(response.text)
    uuid = parsed['data'][0]['process_uuid']
    url = f'https://broadbandmap.fcc.gov/nbm/map/api/national_map_process/nbm_get_data_download/{uuid}'
    response = s.get(url)
    parsed = json.loads(response.text)
    data = pd.DataFrame(parsed['data'])
    data = data[data['technology_code'].isin(['10','40', '50', '71', '300', '400', '500'])]
    data = data[data['data_type'].isin(['Mobile Broadband', 'Fixed Broadband'])]
    data = data[data['data_category'].isin(['Nationwide'])]
    downloadList = data.to_dict('records')
    if os.path.isdir('data') == False:
        os.mkdir('data')
    cachedFiles = os.listdir('data')
    cachedFiles = [entry for entry in cachedFiles if entry.endswith(".zip")]
    cachedFileNames = [x.split('.')[0] for x in cachedFiles]
    print('Download Progress')
    for item in tqdm(downloadList):
        url = f"https://broadbandmap.fcc.gov/nbm/map/api/getNBMDataDownloadFile/{item['id']}/1"
        if item['file_name'] not in cachedFileNames:
            # print(item['file_name'])
            retryableErrors = [requests.exceptions.ChunkedEncodingError]
            r = retry_call(s.get, fkwargs={'url': url}, exceptions=retryableErrors, tries=2, delay=2, backoff=5)
            # r = downloadWrapper(s, url)
            open(f'data/{item["file_name"]}.zip', 'wb').write(r.content)
    pass

def prep():
    fileList = os.listdir('data')
    fileList = [item for item in fileList if item.endswith(".zip")]
    skipTech = ['_3G_', '_4G-LTE_', '_5G-NR_']
    print('Unzipping Data')
    for file in tqdm(fileList):
        if not any(ele in file for ele in skipTech):
            shutil.unpack_archive(f'data/{file}', 'data')
            pass
        else:
            shutil.unpack_archive(f'data/{file}', f'data/{file[0:-4]}')
            pass
    fileList = os.listdir('data')
    fileList = [item for item in fileList if item.endswith(".csv")]
    techTypes = ['Cable', 'Copper', 'Fiber-to-the-Premises', 'Licensed-Fixed-Wireless']

    data = {}

    print('build lists')
    for tech in tqdm(techTypes):
        data[tech] = dict()
        data[tech]['files'] = list()
        for file in fileList:
            if tech in file:
                data[tech]['files'].append(file)

    print('processing technologies')
    for tech in data.keys():
        print(tech)
        dfCountryBlock = pd.DataFrame()
        dfCountryH3 = pd.DataFrame()
        for file in tqdm(data[tech]['files']):
            dfState = pd.read_csv(f'data/{file}')
            processed = processData(dfState)
            dfCountryBlock = dfCountryBlock.append(processed['block'], ignore_index=True)
            dfCountryH3 = dfCountryH3.append(processed['h3_8'], ignore_index=True)
        dfCountryBlock.describe()
        data[tech]['df'] = dict()
        data[tech]['df']['block'] = dfCountryBlock
        data[tech]['df']['h3_8'] = dfCountryH3
    
    start = datetime.now()
    data['Cable']['df']['block'].to_csv('countryBlockCable.csv')
    data['Cable']['df']['h3_8'].to_csv('countryH38Cable.csv')
    data['Copper']['df']['block'].to_csv('countryBlockCopper.csv')
    data['Copper']['df']['h3_8'].to_csv('countryH38Copper.csv')
    data['Fiber-to-the-Premises']['df']['block'].to_csv('countryBlockFttp.csv')
    data['Fiber-to-the-Premises']['df']['h3_8'].to_csv('countryH38Fttp.csv')
    data['Licensed-Fixed-Wireless']['df']['block'].to_csv('countryBlockLfw.csv')
    data['Licensed-Fixed-Wireless']['df']['h3_8'].to_csv('countryH38Lfw.csv')
    end = datetime.now()
    print(f'Time spent saving: {end-start}')
    pass

def main():
    start = datetime.now()
    download()
    prep()
    end = datetime.now()
    print(f'Total time elapsed: {end-start}')
    pass

if __name__ == "__main__":
    main()