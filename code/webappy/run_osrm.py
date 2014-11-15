import subprocess
import os

def run_osrm():
    os.chdir('../../osrm-backend/build/')
    subprocess.call('./osrm-routed new-york-latest.osrm -p 6969&', shell=True)