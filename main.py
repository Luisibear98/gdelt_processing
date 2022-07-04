from processing import *


gdelt_base_url = 'http://data.gdeltproject.org/events/'
#Be careful with this path
local_path = './downloads/'
#FIPS code Spain 
fips_country_code = 'SP'


file_list = scrap_files(gdelt_base_url)
download_files(local_path,file_list,fips_country_code,gdelt_base_url,maximum_read=1000)
process_df(local_path,fips_country_code)