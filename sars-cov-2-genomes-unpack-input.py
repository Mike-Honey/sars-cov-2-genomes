import datetime
import os
import pandas
import tarfile


def main():
    """
    Main - program execute
    """
    print (str(datetime.datetime.now()) + ' Starting ...')
    downloaddir = 'C:/Users/mikehoney/Downloads/'
    datadir = 'C:/Dev/sars-cov-2-genomes/'
    
    for file in os.listdir(downloaddir):
        filename = os.fsdecode(file)
        # process each input .tar.xz file.
        if filename.startswith('metadata_tsv_') and filename.endswith('.tar.xz'):
            print (str(datetime.datetime.now()) + ' Opening: ' + filename)
            tar = tarfile.open(downloaddir + filename, "r:xz")
            for tarinfo in tar:
                print(tarinfo.name, "is", tarinfo.size, "bytes in size and is ", end="")
                if tarinfo.isreg():
                    print("a regular file.")
                elif tarinfo.isdir():
                    print("a directory.")
                else:
                    print("something else.")
            print (str(datetime.datetime.now()) + ' Extracting: ' + filename)
            tar.extractall(path=datadir)
            tar.close()
            print (str(datetime.datetime.now()) + ' Finished unzip of ' + filename )

    # initialise a list to hold chunks of the extracted tsv file (13GB+)
    metadata_latest = [] 

    # read the extracted tsv file in chunks of 1M rows and process each chunk
    with pandas.read_csv(datadir + 'metadata.tsv', delimiter='\t', iterator=True, chunksize=1000000) as metadata_tsv:
    # Filter the data accordingly.
        print (str(datetime.datetime.now()) + ' Filtering by Collection date ...' )
        for metadata_chunk in metadata_tsv:
            print (str(datetime.datetime.now()) + ' Processing a 1M row chunk of metadata.tsv ...' )
            # filter each chunk and append the resulting dataframe to the list.
            metadata_latest.append(metadata_chunk[metadata_chunk['Collection date'] > '2023-12-22'])

    print (str(datetime.datetime.now()) + ' Writing metadata_latest.tsv ...' )
    # concatenate all the filtered dataframes together and write to the output file.
    pandas.concat(metadata_latest).to_csv(datadir + 'metadata_latest.tsv', sep='\t')
    print (str(datetime.datetime.now()) + ' Finished writing of metadata_latest.tsv' )


    print (str(datetime.datetime.now()) + ' Finished!')
    exit()

if __name__ == '__main__':
    main()
