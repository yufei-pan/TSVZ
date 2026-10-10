- One record per line; a line counts only once its `\n` is written. §4.3
- Field 0 is the key; the last line for a key wins. §3.3
- A key alone, with no delimiter, deletes it. §9
- `#` lines are comments; `#_name_#` lines are markers. §11, §12
- `<sep>`, `<LF>`, `<lt>` and `<#>` stand for the delimiter, a newline, `<`
  and `#`. §13
- The extension picks the delimiter: `.tsvz` tab, `.csvz` comma, `.nsvz` NUL,
  `.psvz` pipe. Plain `.tsv` and `.csv` files are loose tables. §5
