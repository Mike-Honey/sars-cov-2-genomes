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

    metadata_latest = pandas.read_csv(datadir + 'metadata.tsv', delimiter='\t')
    # Filter the data accordingly.
    metadata_latest = metadata_latest[metadata_latest['Collection date'] > '2023-12-22']
    metadata_latest.to_csv(datadir + 'metadata_latest.tsv', sep='\t')
    print (str(datetime.datetime.now()) + ' Finished writing of metadata_latest.tsv' )


    print (str(datetime.datetime.now()) + ' Finished!')
    exit()

if __name__ == '__main__':
    main()
