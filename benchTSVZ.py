#!/usr/bin/env python3
"""Throughput benchmark for the spec TSVZ engine (TSVZ_new)."""
import argparse
import os
import random
import re
import shutil
import time

import TSVZ_new as TSVZ

RESOURCE_LIB_AVAILABLE = True
try:
	import resource
except ImportError:
	RESOURCE_LIB_AVAILABLE = False

version = '3.0'


def almost_urandom(n):
	try:
		return random.getrandbits(8 * n).to_bytes(n, 'big').decode(errors='replace')
	except OverflowError:
		return almost_urandom(n // 2) + almost_urandom(n - n // 2)


def get_terminal_size():
	try:
		return os.get_terminal_size()
	except Exception:
		try:
			import fcntl
			import struct
			import termios
			packed = fcntl.ioctl(0, termios.TIOCGWINSZ, struct.pack('HHHH', 0, 0, 0, 0))
			return struct.unpack('HHHH', packed)[:2]
		except Exception:
			return shutil.get_terminal_size(fallback=(240, 50))


def format_bytes(size, use_1024_bytes=None, to_int=False, to_str=False, str_format='.2f'):
	if to_int or isinstance(size, str):
		if isinstance(size, int):
			return size
		if isinstance(size, str):
			match = re.match(r"(\d+(\.\d+)?)\s*([a-zA-Z]*)", size)
			if not match:
				if to_str:
					return size
				print("Invalid size format. Expected format: 'number [unit]', e.g., '1.5 GiB' or '1.5GiB'")
				print(f"Got: {size}")
				return 0
			number, _, unit = match.groups()
			number = float(number)
			unit = unit.strip().lower().rstrip('b')
			if unit.endswith('i'):
				use_1024_bytes = True
			elif use_1024_bytes is None:
				use_1024_bytes = False
			unit = unit.rstrip('i')
			power = 2**10 if use_1024_bytes else 10**3
			unit_labels = {'': 0, 'k': 1, 'm': 2, 'g': 3, 't': 4, 'p': 5}
			if unit not in unit_labels:
				if to_str:
					return size
				print(f"Invalid unit '{unit}'. Expected one of {list(unit_labels.keys())}")
				return 0
			if to_str:
				return format_bytes(
					size=int(number * (power ** unit_labels[unit])),
					use_1024_bytes=use_1024_bytes, to_str=True, str_format=str_format,
				)
			return int(number * (power ** unit_labels[unit]))
		try:
			return int(size)
		except Exception:
			return 0
	if to_str or isinstance(size, (int, float)):
		if isinstance(size, str):
			try:
				size = float(size.rstrip('B').rstrip('b').lower().strip())
			except Exception:
				return size
		if use_1024_bytes or use_1024_bytes is None:
			power = 2**10
			n = 0
			power_labels = {0: '', 1: 'Ki', 2: 'Mi', 3: 'Gi', 4: 'Ti', 5: 'Pi'}
			while size >= power:
				size /= power
				n += 1
			return f"{size:{str_format}} {power_labels[n]}"
		power = 10**3
		n = 0
		power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T', 5: 'P'}
		while size >= power:
			size /= power
			n += 1
		return f"{size:{str_format}} {power_labels[n]}"
	try:
		return format_bytes(float(size), use_1024_bytes)
	except Exception as e:
		print(f"Error: {e}")
		print(f"Invalid size: {size}")
	return 0


def get_resource_usage(return_dict=False):
	try:
		if RESOURCE_LIB_AVAILABLE:
			raw = resource.getrusage(resource.RUSAGE_SELF)
			resource_dict = {
				'user mode time': f'{raw.ru_utime} seconds',
				'system mode time': f'{raw.ru_stime} seconds',
				'max resident set size': f'{format_bytes(raw.ru_maxrss * 1024)}B',
				'shared memory size': f'{format_bytes(raw.ru_ixrss * 1024)}B',
				'unshared memory size': f'{format_bytes(raw.ru_idrss * 1024)}B',
				'unshared stack size': f'{format_bytes(raw.ru_isrss * 1024)}B',
				'cached page hits': f'{raw.ru_minflt}',
				'missed page hits': f'{raw.ru_majflt}',
				'swapped out page count': f'{raw.ru_nswap}',
				'block input operations': f'{raw.ru_inblock}',
				'block output operations': f'{raw.ru_oublock}',
				'IPC messages sent': f'{raw.ru_msgsnd}',
				'IPC messages received': f'{raw.ru_msgrcv}',
				'signals received': f'{raw.ru_nsignals}',
				'voluntary context sw': f'{raw.ru_nvcsw}',
				'involuntary context sw': f'{raw.ru_nivcsw}',
			}
			if return_dict:
				return resource_dict
			return '\n'.join('\t'.join(line) for line in resource_dict.items())
	except Exception as e:
		print(f"Error: {e}")
	return {} if return_dict else ''


def pretty_format_table(data, delimiter='\t', header=None, full=False):
	def visible_len(s):
		return len(re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", s))

	def table_width(col_widths, sep_len):
		return sum(col_widths) + sep_len * (len(col_widths) - 1)

	def truncate_to_width(s, width):
		if visible_len(s) <= width:
			return s
		if width <= 0:
			return ''
		return s[:max(width - 2, 0)] + '..'

	if not data:
		return ''
	if isinstance(data, str):
		data = data.strip('\n').split('\n')
		data = [line.split(delimiter) for line in data]
	elif isinstance(data, dict):
		if isinstance(next(iter(data.values())), dict):
			temp = [['key'] + list(next(iter(data.values())).keys())]
			temp.extend([[key] + list(value.values()) for key, value in data.items()])
			data = temp
		else:
			data = [[key] + list(value) for key, value in data.items()]
	elif not isinstance(data, list):
		data = list(data)
	if isinstance(data[0], dict):
		temp = [list(data[0].keys())]
		temp.extend([list(item.values()) for item in data])
		data = temp
	data = [[str(item) for item in row] for row in data]
	num_cols = len(data[0])
	if header is None:
		header = data[0]
		rows = data[1:]
	else:
		if isinstance(header, str):
			header = header.split(delimiter)
		if len(header) < num_cols:
			header = header + [''] * (num_cols - len(header))
		elif len(header) > num_cols:
			header = header[:num_cols]
		rows = data

	def compute_col_widths(hdr, rows_):
		col_w = [0] * len(hdr)
		for i in range(len(hdr)):
			col_w[i] = max(0, visible_len(hdr[i]), *(visible_len(r[i]) for r in rows_ if i < len(r)))
		return col_w

	normalized_rows = []
	for r in rows:
		if len(r) < num_cols:
			r = r + [''] * (num_cols - len(r))
		elif len(r) > num_cols:
			r = r[:num_cols]
		normalized_rows.append(r)
	rows = normalized_rows
	col_widths = compute_col_widths(header, rows)
	sep = ' | '
	hsep = '-+-'
	cols = get_terminal_size()[0]

	def render(hdr, rows_, col_w, sep_str, hsep_str):
		row_fmt = sep_str.join('{{:<{}}}'.format(w) for w in col_w)
		out = [row_fmt.format(*hdr), hsep_str.join('-' * w for w in col_w)]
		for row in rows_:
			if not any(row):
				out.append(hsep_str.join('-' * w for w in col_w))
			else:
				row = [truncate_to_width(row[i], col_w[i]) for i in range(len(row))]
				out.append(row_fmt.format(*row))
		return '\n'.join(out) + '\n'

	if full:
		return render(header, rows, col_widths, sep, hsep)
	if table_width(col_widths, len(sep)) <= cols:
		return render(header, rows, col_widths, sep, hsep)
	sep = '|'
	hsep = '+'
	if table_width(col_widths, len(sep)) <= cols:
		return render(header, rows, col_widths, sep, hsep)
	header_widths = [visible_len(h) for h in header]
	width_diff = [max(col_widths[i] - header_widths[i], 0) for i in range(num_cols)]
	total_overflow_width = table_width(col_widths, len(sep)) - cols
	for i, diff in sorted(enumerate(width_diff), key=lambda x: -x[1]):
		if total_overflow_width <= 0:
			break
		if diff <= 0:
			continue
		reduce_by = min(diff, total_overflow_width)
		col_widths[i] -= reduce_by
		total_overflow_width -= reduce_by
	return render(header, rows, col_widths, sep, hsep)


def _open_store(path, *, lite=False, flush_interval=0.01, verbose=False):
	if lite:
		return TSVZ.OffsetStore(path, create=True)
	return TSVZ.WalStore(path, create=True, flush_interval=flush_interval)


def _print_usage(verbose):
	if verbose:
		print(get_resource_usage())


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Benchmark for TSVZ (spec engine)')
	parser.add_argument('file_name', type=str, help='Part file to benchmark')
	parser.add_argument('-n', '--number', type=int, default=1_000_000,
						help='Number of entries to write (default: 1M)')
	parser.add_argument('--lite', action='store_true',
						help='Use OffsetStore instead of WalStore')
	parser.add_argument('--snapshot', action='store_true',
						help='Run snapshot_part after writes (§19 compaction)')
	parser.add_argument('--flush-interval', type=float, default=0.01,
						help='WalStore background flush interval in seconds')
	parser.add_argument('-v', '--verbose', action='store_true',
						help='Print resource usage and extra detail')
	parser.add_argument('-V', '--version', action='version', version=f'%(prog)s {version}')
	args = parser.parse_args()

	store_label = 'OffsetStore' if args.lite else 'WalStore'
	start = time.perf_counter()
	store = _open_store(
		args.file_name, lite=args.lite, flush_interval=args.flush_interval, verbose=args.verbose,
	)
	print(f'Time to create / load {store_label}: {time.perf_counter() - start:.3f} seconds')
	_print_usage(args.verbose)

	start = time.perf_counter()
	for i in range(args.number):
		store[str(i)] = [str(i)] + [str(id(i))] * 19
	if hasattr(store, 'close'):
		store.close()
	else:
		store.flush()
	del store
	elapsed = time.perf_counter() - start
	print(f'Time to write {args.number} entries: {elapsed:.3f} seconds')
	if args.number:
		print(f'Rate: {args.number / elapsed:,.0f} entries/s')
	_print_usage(args.verbose)

	if args.snapshot:
		start = time.perf_counter()
		TSVZ.snapshot_part(args.file_name)
		print(f'Time to snapshot {args.number} entries: {time.perf_counter() - start:.3f} seconds')
		_print_usage(args.verbose)
