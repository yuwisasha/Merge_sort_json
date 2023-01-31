import jsonlines

import time
import argparse
import shutil

from pathlib import Path
from pathlib import PosixPath

_MERGED_LOGS = 'merged_logs.jsonl'
_SORT_KEY = 'timestamp'


class Invalid_Number_of_paths(Exception):
    pass

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Tool to merge logs')

    parser.add_argument(
        'input_paths',
        help='paths to logs, must be 2',
        metavar='<INPUT PATHS>',
        nargs='+',
        type=PosixPath,
    )

    parser.add_argument(
        '-f', '--force',
        action='store_const',
        const=True,
        default=False,
        dest='force_merge',
        help='force merge logs',
    )

    parser.add_argument(
        '-o',
        metavar='<OUTPUT DIR>',
        type=str,
        help='path to dir with merged logs',
        dest='output_dir',
    )

    return parser.parse_args()


def _validate_input_paths(input_paths : list) -> None:
    if len(input_paths) != 2:
        raise Invalid_Number_of_paths(
            'Incorrect usage. Must be 2 paths.')

    for path in input_paths:
        if not path.exists():
            raise FileExistsError(
                f"Path {path} doesn't exists.")


def _create_dir(dir_path: Path, *, force_merge: bool = False) -> None:
    if dir_path.exists():
        if not force_merge:
            raise FileExistsError(
                f'Dir "{dir_path}" already exists. Remove it first or choose another one.')
        shutil.rmtree(dir_path)

    dir_path.mkdir(parents=True)

def _merge_logs(output_dir : Path, input_paths : list, sort_key : str) -> None:
    output_log_filename = output_dir.joinpath(_MERGED_LOGS)
    print(f'Merging {input_paths[0]} with {input_paths[1]} ...')

    path_to_log_1 = input_paths[0]
    path_to_log_2 = input_paths[1]

    with jsonlines.open(output_log_filename, mode='w') as writer:
        with jsonlines.open(path_to_log_1) as reader_1:
            with jsonlines.open(path_to_log_2) as reader_2:
                try:
                    log_1 = reader_1.read()
                except EOFError:
                    log_1 = {f'{sort_key}' : ''}

                try:
                    log_2 = reader_2.read()
                except EOFError:
                    log_2 = {f'{sort_key}' : ''}

                while log_1[sort_key] or log_2[sort_key]:
                    if log_1[sort_key] <= log_2[sort_key] and log_1[sort_key] or not log_2[sort_key]:
                        writer.write(log_1)
                        try:
                            log_1 = reader_1.read()
                        except EOFError:
                            log_1 = {f'{sort_key}' : ''}
                    else:
                        writer.write(log_2)
                        try:
                            log_2 = reader_2.read()
                        except EOFError:
                            log_2 = {f'{sort_key}' : ''}  


def main() -> None:
    args = _parse_args()

    t0 = time.time()
    input_paths = args.input_paths
    output_dir = Path(args.output_dir)
    _validate_input_paths(input_paths)
    _create_dir(output_dir, force_merge=args.force_merge)
    _merge_logs(output_dir, input_paths, sort_key=_SORT_KEY)
    print(f'finished in {time.time() - t0:0f} sec')


if __name__ == '__main__':
    main()
