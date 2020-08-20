`hiveextractor` is a tool for exporting HIVE tables into GBQ  

It uses `pyHS2` - a python client driver for connecting to hive server 2.  
  
set `GOOGLE_APPLICATION_CREDENTIALS` env variable for GCP authentication  

# Config  
> use `config.py` to manage tables for extraction  

# Build  
> python hiveextractor/setup.py bdist_wheel  

# Setup  
> apt install libsasl2-dev  
python -m pip install hiveextractor-1.0.0-py2-none-any.whl  

# Run  
> extract_start --host=<HIVE_HOST_ADDRESS> --db=<HIVE_DB_NAME> --creds=<USERNAME,PASSWORD>  
export_start --db=<HIVE_DB_NAME> [ --tables=<HIVE_TABLES_TO_EXPORT> ]  

*tested on Python 2.7.17 @ Ubuntu 18.04.5 LTS (Bionic Beaver)*  