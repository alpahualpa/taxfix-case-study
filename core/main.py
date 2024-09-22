# Author: Nikita Silin
# Task: Taxfix Case Study
# Purpose: Main executable file

from import_api_data import data_importer
from calculate_metrics import generate_report

if __name__ == '__main__':
    data_importer()

    generate_report()