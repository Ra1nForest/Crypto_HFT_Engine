#!/usr/bin/env python3
#
# bilingual_comments.py
# 中英双语注释工具：C++ 源文件里每条中文 // 注释的下一行（或几行）是它的英文翻译。
# Bilingual comment tool: in the C++ sources every Chinese // comment is followed by its English translation.
#
"""Bilingual (Chinese + English) comments in the C++ sources.

  bilingual_comments.py check FILE...                 list Chinese comments without an English line after them
  bilingual_comments.py extract [--force] FILE...      JSON on stdout: the comments that still need a translation
  bilingual_comments.py apply [--force] JSON...        insert the English lines from translation files

Units:
  block     a run of consecutive full-line // comments that contain Chinese, at the same column. The English
            lines go right after the run, at the same indentation.
  trailing  a // comment with Chinese after code. The English goes on the next line(s), starting at the
            column of that comment's //.
Translation JSON: {"path": {"<unit id>": ["English line", ...]}}; a unit id is "<first line>#<content hash>".
--force ignores the "already translated" test (for files that have no English yet, where a separator or a code
example right after a Chinese comment would look like a translation).
A small lexer skips string, character and raw string literals, so "wss://..." or R"(// ...)" are not comments.
"""
import hashlib
import json
import re
import sys

# 中文字符（CJK 统一表意文字、标点与全角字符）
# Chinese characters (CJK unified ideographs, punctuation and full-width forms)
CJK = re.compile(r'[　-〿㐀-䶿一-鿿＀-￯]')
# 只由分隔符组成的注释行（如 // ----、// ====、方框线）
# Comment lines made only of separators (// ----, // ====, box drawing)
SEPARATOR = re.compile(r'^[\s\-=_*#/~.─━│┌┐└┘├┤┬┴┼]*$')


def has_cjk(s):
    return bool(CJK.search(s))


def is_digit_separator(text, i):
    # text[i] 处的单引号是数字分隔符（1'000'000、0xFF'FF）而不是字符字面量：前面的记号以数字开头。
    # u8'x'、u'x'、U'x'、L'x' 的前缀开始的是字符字面量。
    # The quote at text[i] is a digit separator (1'000'000, 0xFF'FF), not a character literal, when the token
    # before it starts with a digit. The prefixes of u8'x', u'x', U'x' and L'x' start character literals.
    j = i
    while j > 0 and (text[j - 1].isalnum() or text[j - 1] in "_'"):
        j -= 1
    token = text[j:i]
    return bool(token) and token[0].isdigit()


def find_line_comments(text):
    # 返回 {行号（从 1 开始）: (// 所在列, // 之后的文本, 该行 // 之前是否只有空白)}
    # Returns {line number (from 1): (column of the //, text after the //, whether only blanks precede it)}
    out = {}
    i, n, line, col0 = 0, len(text), 1, 0  # col0：当前行行首的下标
                                           # col0: index where the current line starts
    while i < n:
        c = text[i]
        if c == '\n':
            line += 1
            col0 = i + 1
            i += 1
            continue
        if c == '/' and i + 1 < n and text[i + 1] == '/':
            j = text.find('\n', i)
            j = n if j < 0 else j
            out[line] = (i - col0, text[i + 2:j], text[col0:i].strip() == '')
            i = j
            continue
        if c == '/' and i + 1 < n and text[i + 1] == '*':
            j = text.find('*/', i + 2)
            j = n if j < 0 else j + 2
            line += text.count('\n', i, j)
            k = text.rfind('\n', i, j)
            col0 = k + 1 if k >= 0 else col0
            i = j
            continue
        # 原始字符串字面量：可选前缀（u8、u、U、L），然后是 R"分隔符( ... )分隔符"
        # Raw string literal: an optional prefix (u8, u, U, L), then R"delim( ... )delim"
        m = re.match(r'(?:u8|u|U|L)?R"([^()\\\s]{0,16})\(', text[i:i + 24]) if c in 'uULR' else None
        if m and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == '_')):
            delim = m.group(1)
            end = text.find(')' + delim + '"', i + m.end())
            end = n if end < 0 else end + len(delim) + 2
            line += text.count('\n', i, end)
            k = text.rfind('\n', i, end)
            col0 = k + 1 if k >= 0 else col0
            i = end
            continue
        if c == '"' or (c == "'" and not is_digit_separator(text, i)):
            j = i + 1
            while j < n and text[j] != c and text[j] != '\n':
                j += 2 if text[j] == '\\' else 1
            i = j + 1 if j < n and text[j] == c else j  # 未闭合的字面量停在换行符之前
                                                        # an unterminated literal stops before the newline
            continue
        i += 1
    return out


def units_for(path):
    text = open(path, encoding='utf-8').read()
    lines = text.split('\n')
    comments = find_line_comments(text)
    units = []
    ln = 1
    while ln <= len(lines):
        info = comments.get(ln)
        if info is None or not has_cjk(info[1]):
            ln += 1
            continue
        col, body, full = info
        if lines[ln - 1].rstrip().endswith('\\'):
            units.append({'kind': 'continuation', 'first': ln, 'last': ln, 'text': [body.strip()]})
        elif not full:
            units.append({'kind': 'trailing', 'first': ln, 'last': ln, 'col': col,
                          'code': lines[ln - 1][:col].rstrip()[-80:], 'text': [body.strip()]})
        else:
            first = ln
            texts = [body[1:] if body.startswith(' ') else body]
            while ln + 1 <= len(lines):
                nxt = comments.get(ln + 1)
                if nxt is None or not nxt[2] or nxt[0] != col or not has_cjk(nxt[1]):
                    break
                if lines[ln].rstrip().endswith('\\'):
                    break
                ln += 1
                texts.append(nxt[1][1:] if nxt[1].startswith(' ') else nxt[1])
            units.append({'kind': 'block', 'first': first, 'last': ln, 'col': col,
                          'indent': lines[first - 1][:col], 'text': texts})
        ln += 1
    for u in units:
        u['id'] = f"{u['first']}#{hashlib.sha1(chr(10).join(u['text']).encode('utf-8')).hexdigest()[:8]}"
    return lines, comments, units


def translated_after(lines, comments, u):
    # 单元之后的一行已经是英文翻译：同一列的整行注释、不含中文、不是分隔线、至少有一个英文单词
    # The line after the unit already holds the English: a full-line comment at the same column, without
    # Chinese, not a separator, with at least one English word
    info = comments.get(u['last'] + 1)
    if info is None or not info[2] or has_cjk(info[1]) or info[0] != u['col']:
        return False
    body = info[1].strip()
    return bool(body) and not SEPARATOR.match(body) and re.search(r'[A-Za-z]{2,}', body) is not None


def cmd_check(paths):
    bad = 0
    for p in paths:
        lines, comments, units = units_for(p)
        for u in units:
            if u['kind'] == 'continuation':
                print(f'{p}:{u["first"]}: Chinese comment on a macro continuation line')
                bad += 1
            elif not translated_after(lines, comments, u):
                print(f'{p}:{u["first"]}: no English line after: {" ".join(u["text"])[:80]}')
                bad += 1
    print(f'{bad} problem(s)')
    return 1 if bad else 0


def cmd_extract(paths, force):
    result = {}
    for p in paths:
        lines, comments, units = units_for(p)
        todo = [u for u in units
                if u['kind'] != 'continuation' and (force or not translated_after(lines, comments, u))]
        if todo:
            result[p] = [{k: u[k] for k in ('id', 'kind', 'text', 'col', 'code') if k in u} for u in todo]
    json.dump(result, sys.stdout, ensure_ascii=False, indent=1)
    print()


def cmd_apply(json_paths, force):
    data = {}
    for jp in json_paths:
        for p, trans in json.load(open(jp, encoding='utf-8')).items():
            data.setdefault(p, {}).update(trans)
    total = 0
    for p, trans in data.items():
        lines, comments, units = units_for(p)
        by_id = {u['id']: u for u in units}
        inserts = []
        for uid, english in trans.items():
            u = by_id.get(uid)
            if u is None:
                raise SystemExit(f'{p}: no Chinese comment matches unit {uid} (was the file edited?)')
            if u['kind'] == 'continuation' or (not force and translated_after(lines, comments, u)):
                continue
            english = [x.rstrip() for x in (english if isinstance(english, list) else [english]) if x.strip()]
            if not english or any(has_cjk(x) for x in english):
                raise SystemExit(f'{p}:{uid}: the translation is empty or contains Chinese')
            pad = u['indent'] if u['kind'] == 'block' else ' ' * u['col']
            inserts.append((u['last'], [pad + '// ' + (x if u['kind'] == 'block' else x.strip()) for x in english]))
        for last, new in sorted(inserts, key=lambda t: -t[0]):  # 自下而上插入，前面的行号不变
            lines[last:last] = new                               # bottom-up, so earlier line numbers stay valid
        open(p, 'w', encoding='utf-8').write('\n'.join(lines))
        total += len(inserts)
        print(f'{p}: {len(inserts)} translation(s) inserted')
    print(f'total {total}')


if __name__ == '__main__':
    if len(sys.argv) < 3 or sys.argv[1] not in ('check', 'extract', 'apply'):
        print(__doc__)
        sys.exit(2)
    cmd, args = sys.argv[1], sys.argv[2:]
    force = '--force' in args
    args = [a for a in args if a != '--force']
    if cmd == 'check':
        sys.exit(cmd_check(args))
    if cmd == 'extract':
        cmd_extract(args, force)
    else:
        cmd_apply(args, force)
