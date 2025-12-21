# Camel Ray

Helper scripts for managing the Camel Ray compute cluster. 

# Quick Start

## Installing the Dependencies in a Conda Environment

Create a new conda environment and install the dependencies using the following command:
```bash
conda env create -f environment.yml
conda activate rayenv
```

## Accessing the Camel Ray Manager

Run the Python script to access the Camel Ray manager:
```bash
python camelray.py
```

Before running the script, you need to add your credentials to the `.env` file that is not synced to the repository.
```env
USERNAME=your_username
PASSWORD=your_password
```

`workers.csv` is a CSV file that contains the information about the workers in the cluster. If any of the workers are moved to a new location or ethernet port, you need to update the `workers.csv` file with the new information.
