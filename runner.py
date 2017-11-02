
import argparse


def parser_args():
    parser = argparse.ArgumentParser(description='Get a stylometry synthetic data.')
    parser.add_argument('--fragment_size', type=int, help='number of chunks in a fragment')
    parser.add_argument('--num_fragment', type=int, help='number of fragment in a section')
    parser.add_argument('--chunk_size', type=int, help='number of chunk in a fragment')
    parser.add_argument('--num_chunk', type=int, help='number of chunk in a fragment')
    parser.add_argument('--num_chunk_per_section', type=int, help='number of chunk in a section')

    parser.add_argument('--num_paper', type=int, help='number of paper')
    parser.add_argument('--offset', type=int, help="number of chunks between n and n+1 fragment")
    parser.add_argument('--db_name', type=str, nargs='*', help="database name that want to get")
    parser.add_argument('--out_path', type=str, help="output path", default='.')
    return parser.parse_args()


if __name__ == "main":
    arg = parser_args()