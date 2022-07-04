import requests
import lxml.html as lh
import os.path
import urllib
import zipfile
import glob
import operator
import urllib.request
import glob
import pandas as pd

def scrap_files(gdelt_base_url):


    # get the list of all the links on the gdelt file page
    page = requests.get(gdelt_base_url+'index.html')
    doc = lh.fromstring(page.content)
    link_list = doc.xpath("//*/ul/li/a/@href")

    # separate out those links that begin with four digits 
    file_list = [x for x in link_list if str.isdigit(x[0:4])]

    print("Number of files found: "+str(len(file_list)))
    return file_list

def download_files(local_path,file_list,fips_country_code,gdelt_base_url,maximum_read):
    infilecounter = 0
    outfilecounter = 0
    
    for compressed_file in file_list[infilecounter:]:
        print(outfilecounter)
        if outfilecounter == maximum_read:
            break
        try:
            # if we dont have the compressed file stored locally, go get it. Keep trying if necessary.
            while not os.path.isfile(local_path+compressed_file): 
            
                urllib.request.urlretrieve(url=gdelt_base_url+compressed_file, 
                                filename=local_path+compressed_file)
                
            # extract the contents of the compressed file to a temporary directory    
        
            z = zipfile.ZipFile(file=local_path+compressed_file, mode='r')    
            z.extractall(path=local_path+'tmp/')
            
            # parse each of the csv files in the working directory, 
            
            for infile_name in glob.glob(local_path+'tmp/*'):
            
                outfile_name = local_path+'country/'+fips_country_code+'%04i.tsv'%outfilecounter
                
                # open the infile and outfile
                with open(infile_name, mode='r') as infile, open(outfile_name, mode='w') as outfile:
                    for line in infile:
                        # extract lines with our interest country code
                        if fips_country_code in operator.itemgetter(51)(line.split('\t')) and fips_country_code in operator.itemgetter(37)(line.split('\t'))and fips_country_code in operator.itemgetter(44)(line.split('\t')):
                             
                            outfile.write(line)

                    outfilecounter +=1
                    
            
                # delete the temporary file
                os.remove(infile_name)
            infilecounter +=1
            print('done')
        except:
            continue



def process_df(local_path,fips_country_code):


    # Get the GDELT field names from a helper file
    colnames = pd.read_excel('/Users/luisibanezlissen/Desktop/gdelt/CSV.header.fieldids.xlsx', sheet_name=1, 
                            index_col='Column ID')['Field Name']

    # Build DataFrames from each of the intermediary files
    files = glob.glob(local_path+'country/'+fips_country_code+'*')
    DFlist = []
    for active_file in files:
     
        DFlist.append(pd.read_csv(active_file, sep='\t', header=None, dtype=str,
                                names=colnames, index_col=['GLOBALEVENTID']))

    # Merge the file-based dataframes and save a pickle
    DF = pd.concat(DFlist)
    DF.to_pickle(local_path+'backup'+fips_country_code+'.pickle')    
        
    # once everythin is safely stored away, remove the temporary files
    for active_file in files:
        os.remove(active_file)
    print(DF)
    print(len(DF))
    for key in DF.keys():
        print(key)
    DF.to_csv('out.csv', index=False) 
    DF.to_csv("outcompress.csv.gz", 
           index=False, 
           compression="gzip")





