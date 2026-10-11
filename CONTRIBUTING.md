# Contributing

DorraDB is maintained by one person. **Outside contributions are not accepted.**

That means:

- Do not open pull requests — they will be closed without merge.
- Do not send patches, drive-by refactors, or co-maintainer requests.
- Code in this repository is written and landed by the maintainer only.

## Issues

[Issues](https://github.com/valtterisa/dorradb/issues) are welcome when something is wrong, unclear, or broken.

Useful reports include:

- What you expected vs what happened
- Steps to reproduce (CLI commands or a short Go snippet)
- OS (Linux / Windows), Go version, DorraDB commit or tag if you have one

Discussion on design or docs gaps is fine. An issue is not a path to merge your code upstream.

## Fork and self-host

The project is [MIT](LICENSE). You can:

- **Fork** the repo and change anything for your own use or product
- **Clone** and build locally (`go run ./cmd/dorradb`, or import `"dorradb"`)
- **Self-host** the CLI or embed the library with no account, CLA, or upstream approval

Your fork is yours. Changes there stay there unless the maintainer independently reimplements something similar.

## Security

If you believe you’ve found a security issue, open an issue with enough detail to reproduce, or contact the maintainer via the GitHub profile. Do not file a PR with an exploit patch expecting it to be merged.
