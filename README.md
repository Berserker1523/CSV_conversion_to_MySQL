# CSV_conversion_to_MySQL

## How to run

1. Install pip and python

2. Run in the root directory:
```
  pip install -r requirements.txt
```
3. Create a database in MySQL and change the configuration in the line 53(database name) and the line 77 of the read_data.py file.

4. Run the file
```
  python read_data.py
```

## Atypical cases

For duplicate values, those values are stored in a list and later the application calculate the int average of them.

For the missing data, those values are set to null. We could want to calculate an average of the data of that variable and we prefer don't set any missing value to 0.
