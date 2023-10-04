# loder
Pipeline to extract LEHD LODES tables into Postgres

## Setup/Installation

### Dependencies
requires python and postgresql. 

### Installation
#### 1. First, create a virtual environment at the project root.
```shell
python -m venv ve
```
#### 2. Activate it with:
```shell
. ve/bin/activate
```
(or `ve\Scripts\Activate.bat` for windows command prompt)

#### 3. Install python dependencies with:
```shell
pip install -r requirements.txt
```

#### 4. Create a .env file, with these variables. 

```
HOST = "localhost"
UN = "your_postgres_un"
PW = "fake_pw"
PORT = "your_db_port"
```

## Usage
Run the loder.py file. Pick the tables following the prompts in the command. The tables will populate in your DB.

## License
This project uses the GNU(v3) public license.