# DUOS CLI

This application is designed for easing the process of handling CSV data from the DUOS algorithm. Basic functionality for uploading collections of articles or rererences is included.

### Usage

```bash
# show help
$ ./duos.py
Usage: duos.py [OPTIONS] COMMAND [ARGS]...

  utilitiy for loading the data for the DUOS research study.

Options:
  --help  Show this message and exit.

Commands:
  create   create duos database schema in target db.
  destroy  drop every table in duos database.
  info     list basic info about duos db
  upload   insert local csv into the database.
```

`create`,  `destroy`, and `info` are basic convenience tools. The real money is with `upload`.

The `upload` function expects to find CSV files in the local directory whose names match those defined in `CONSTANTS.py` ("article" or "reference" by default). Any eligible files will be normalized and inserted in the target base with little or no discrimination. Only malformed records or files with incorrect metadata will be rejected.

```bash
$ ./duos.py upload
🔍  CSVs discovered: {'articles'}...
💬  Working...
    ....
    .....
    ......
ℹ️  3 records processed.
🙌  done!
```

### Setup

This application assumes that you are using a macOS or unix computer and that you have Python 3.x + pip installed. [Pipenv](https://pipenv.readthedocs.io/en/latest/install/#pragmatic-installation-of-pipenv) is also required to install the necessary dependencies. Installation instructions are included via the link.

1. Clone this repository to your local machine.

2. In the cloned folder, add a `.env` file with connection info corresponding to the Postgres instance you're writing to.
   ```
   DB_HOST=<BEST_HOST_EVER>
   DB_USER=<USERNAME>
   DB_NAME=<PROBABLY_DUOS>
   DB_PASSWORD=<YOUR_AWESOME_PASSWORD>
   DB_PORT=<THE_PERFECT_PORT>
   ```

3. Install application dependencies.

   ```bash
   $ pipenv install
   ```

4. Open a terminal in the folder where you've cloned this tool and defined your `.env`.Make the `duos.py` file executable

   ```bash
   $ chmod +x duos.py
   ```

5. To see available commands, run `./duos.py`. 

