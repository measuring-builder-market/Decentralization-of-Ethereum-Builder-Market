# Decentralization-of-Ethereum-s-Builder-Market

This repository holds the data and code used for the paper "Decentralization of Ethereum's Builder Market".

## Prerequisites

[Python3](https://www.python.org/downloads/) and [Jupyter Notebook](https://jupyter-notebook-beginner-guide.readthedocs.io/en/latest/install.html) are required to reproduce the figures and tables, and you can run `pip install -r requirements.txt` to install the following libraries:

```
fastparquet==2024.5.0
matplotlib==3.9.1.post1
numpy==1.26.4
pandas==2.2.2
seaborn==0.13.2
scipy==1.14.1
scipy==1.14.1
```

## Data

Please download the following files and place them in the `data` folder:

- [eth_blocks.parquet](https://auction-dataset.s3.us-east-2.amazonaws.com/others/eth_blocks.parquet): A Parquet file that stores all blocks and winning bids from historical MEV-Boost auctions between September 2022 and July 2024.
- [private transactions](https://auction-dataset.s3.us-east-2.amazonaws.com/others/private_transactions.parquet): A Parquet file that stores all private order flows and their providers from September 2022 to July 2024.
- [level.db](https://auction-dataset.s3.us-east-2.amazonaws.com/others/level.db): A SQLite database file that stores pivotal providers from historical MEV-Boost auctions between September 2022 and July 2024.


## Structure of the repository

- `data`: The folder for data.
    - `index`: The intermediate results of true values.
    - `builders.json`: The mapping between builder public keys and builder identities.
- `images`: The folder where the output images are stored.
- `pivotal_provider.py`: A script to compute pivotal providers from historical MEV-Boost auctions between September 2022 and July 2024.
- `plot.ipynb`: A Jupyter notebook to reproduce all results in the paper.
- `time_util.py`: A script that defines time utility functions for computing Ethereum slots.
- `validate_bids_representativeness.py`: A script to validate the representativeness of the bids from ultra sound relay.