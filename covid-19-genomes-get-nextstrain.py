import datetime
import pandas
import shutil
import urllib.request
import zstandard 

def process_each_file(datadir, webpageURL, filename):

    print (str(datetime.datetime.now()) + ' Downloading file ' + filename + ' ...')
    with urllib.request.urlopen(webpageURL) as response, open(datadir + filename, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)

    print (str(datetime.datetime.now()) + ' Extracting data from ' + filename + ' ...')

    with open(datadir + filename, "rb") as input_file:
        data = input_file.read()

    dctx = zstandard.ZstdDecompressor()
    decompressed = dctx.decompress(data)

    print (str(datetime.datetime.now()) + ' Extracting data from ' + filename + ' ...')

    with open(datadir + filename.replace('.zst',''), 'wb', buffering=0) as output_file:
        output_file.write(decompressed)


def main():
    """
    Main - program execute
    """
    print (str(datetime.datetime.now()) + ' Starting ...')
    webpageURL = 'https://data.nextstrain.org/files/ncov/open/[each_file].zst'
    filename = '[each_file].zst'
    datadir = 'C:/Dev/sars-cov-2-genomes/'
    file_list = ['metadata.tsv', 'nextclade.tsv']
    
    for each_file in file_list:
        process_each_file(datadir, webpageURL.replace('[each_file]', each_file), filename.replace('[each_file]', each_file))

    print (str(datetime.datetime.now()) + ' Finished!')
    exit()

if __name__ == '__main__':
    main()
