import io
from pathlib import Path


def main() -> None:
    path = Path('tools/live_probe.py')
    text = path.read_text(encoding='utf-8')

    # Split into lines, preserve trailing newline style
    lines = text.splitlines(keepends=True)

    out = []
    in_fetch_token = False
    dedented_body = 0  # how many extra leading tabs we have stripped inside fetch_token body

    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith('def fetch_token():'):
            in_fetch_token = True
            dedented_body = len(line) - len(line.lstrip())
            out.append(line)
            continue

        if in_fetch_token:
            if line.strip() == '':
                out.append(line)
                continue
            # next def or higher indent level ends the block
            current_indent = len(line) - len(line.lstrip())
            if current_indent < dedented_body:
                in_fetch_token = False
                out.append(line)
                continue
            # normalize to exactly dedented_body + 1 tab + original content
            content = line.lstrip()
            normalized = '\t' * (dedented_body + 1) + content
            out.append(normalized)
            continue

        out.append(line)

    path.write_text(''.join(out), encoding='utf-8')

    print(f'wrote {path}', file=sys.stderr)
    print(repr(path.read_text(encoding='utf-8').splitlines()[32:38]), file=sys.stderr)


if __name__ == '__main__':
    main()
