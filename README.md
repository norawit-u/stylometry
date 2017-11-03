# Stylometry

## Hyperparameter
```bash
  -h, --help            show this help message and exit
  --chunk_size CHUNK_SIZE
                        size of the chunk, number of token in the chunk
  --token_size TOKEN_SIZE
                        size of the overall token
  --num_authors NUM_AUTHORS
                        number of real authors
  --num_authors_list NUM_AUTHORS_LIST
                        number of authors including generated one
  --sliding_window SLIDING_WINDOW
                        size of the sliding window
  --num_paper NUM_PAPER
                        number of a paper to create database
```

get_csv.py
```bash
usage: get_csv.py [-h] [--fragment_size FRAGMENT_SIZE]
                  [--num_fragment NUM_FRAGMENT] [--chunk_size CHUNK_SIZE]
                  [--num_chunk NUM_CHUNK]
                  [--num_chunk_per_section NUM_CHUNK_PER_SECTION]
                  [--num_paper NUM_PAPER] [--offset OFFSET]
                  [--db_name [DB_NAME [DB_NAME ...]]]
                  [--out_path [OUT_PATH [OUT_PATH ...]]]

Create a stylometry synthetic dataset.

optional arguments:
  -h, --help            show this help message and exit
  --fragment_size FRAGMENT_SIZE
                        number of chunks in a fragment
  --num_fragment NUM_FRAGMENT
                        number of fragment in a section
  --chunk_size CHUNK_SIZE
                        number of chunk in a fragment
  --num_chunk NUM_CHUNK
                        number of chunk in a fragment
  --num_chunk_per_section NUM_CHUNK_PER_SECTION
                        number of chunk in a section
  --num_paper NUM_PAPER
                        number of paper
  --offset OFFSET       number of chunks between n and n+1 fragment
  --db_name [DB_NAME [DB_NAME ...]]
                        database name that want to get
  --out_path [OUT_PATH [OUT_PATH ...]]
                        output path
```

## Run example

gat_csv.py

```bash
> python get_csv.py --db_name syn_eng_max_while_np1000_c600_t8000_a2_al2_sw200 --out_path csv --num_paper 1000
```

experiment.py

```bash
> python experiment.py --input csv/syn_eng_max_while_np1000_c600_t8000_a2_al2_sw200.csv --output_path out --num_fragment 4000
```

gengraph.py

```bash
> python gengraph.py --num_authors 2  --num_authors_list 3 --num_paper 1000 --db_name syn_eng_max_while_np1000_c600_t8000_a2_al3_sw200  --dir_path /home/cpeuser/cpehk01/tle/FastLSH-Multiauthor/out_max1000/syn_eng_max_while_np1000_c600_t8000_a2_al3_sw200/
```

runner.py

```bash
> 
```
```
## Diagram


||||  Generate Database(gendb_english.py) |||
|:-:|:-:|:-:|:-:|:-:|:-:|
|number of paper  |	chunk size  |	token size  |	author  | 	author list |	sliding window  | |


|||| Database ||||
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|| paper ||| chunk  ||

## Calculation

  number_of_chunk = number_of_paper x token_size / sliding_window
  number_of_chunk = number_of_features / 57

run experiment
```bash
python experiments_simple_min_p3.py 10_syn_eng_max_while_np1000_c600_t8000_a2_al2_sw200 4000
```
run gengraph
```bash
python2 gengraph2_simple.py --num_authors 2  --num_authors_list 2 --num_paper 1000 --db_name syn_eng_max_while_np1000_c600_t8000_a2_al2_sw200  --dir_path /home/cpeuser/cpehk01/tle/FastLSH-Multiauthor/out_max1000/10_syn_eng_max_while_np1000_c600_t8000_a2_al2_sw200/
```