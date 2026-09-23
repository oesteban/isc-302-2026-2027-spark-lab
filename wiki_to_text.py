#!/usr/bin/env python3
"""Turn a MediaWiki XML dump into one plain-text article per line.

The point of this script is not the text. It is the *shape* of what comes out.
Monday's corpus was 18 846 small files, and Spark gave one input partition per
file. This script produces a single compressed stream instead, so the same word
count over the same amount of text reads a different execution plan. That
difference is the whole of round 1.

It is deliberately dependency-free: the standard library parses XML and writes
bzip2, so it runs on whatever Python a laptop already has.

    python3 wiki_to_text.py simplewiki.xml.bz2 data/simplewiki.txt.bz2

Output is one article per line: the title, a tab, then the body with the markup
removed and every run of whitespace collapsed to a single space. One line per
article matters, because Spark splits text on newlines and an article that
spanned lines would be counted as several documents.
"""
import bz2
import re
import sys
import xml.etree.ElementTree as ET

# MediaWiki markup, removed in this order. Templates and tables nest, so those
# two are applied repeatedly until the text stops shrinking.
RE_COMMENT = re.compile(r"<!--.*?-->", re.S)
RE_REF = re.compile(r"<ref[^>]*?/>|<ref[^>]*?>.*?</ref>", re.S | re.I)
RE_MATH = re.compile(r"<math[^>]*>.*?</math>", re.S | re.I)
# Inside <gallery> an image is written bare, with no [[ ]] around it, so the
# bracket rules never see it and RE_TAG would leave the filenames as prose.
RE_BLOCK = re.compile(
    r"<(gallery|imagemap|timeline|syntaxhighlight|source|pre)[^>]*>.*?</\1>",
    re.S | re.I)
RE_FILETOKEN = re.compile(r"\b(?:File|Image):[^\s|\]]+", re.I)
RE_TAG = re.compile(r"<[^>]+>")
# Templates are removed by a brace scanner, not a regex: a stray single brace
# inside one (MediaWiki allows "state={ }") makes a balanced-pair pattern
# unmatchable, and the outer template then survives as prose. Two hundred and
# thirty such lines put "bgcolor" and "fefefe" in the top fifteen words.
# Wiki tables nest, and a regex that stops at the first "|}" leaves the outer
# table's attributes behind as prose. Those attributes are why an early run of
# this script put "bgcolor", "align" and "fefefe" in the top fifteen words.
RE_TABLE_OPEN = re.compile(r"^\s*\{\|")
RE_TABLE_CLOSE = re.compile(r"^\s*\|\}")
# A table row or header cell. Prose never begins a line with "|" or "!", so this
# catches rows whose table opener was consumed by an enclosing template and is
# no longer there to switch the depth counter on.
RE_TABLE_ROW = re.compile(r"^\s*[|!]")
# These three match only brackets with nothing bracketed inside them, so the
# loop in strip_markup() resolves a nested link from the inside out. A caption
# like [[File:x.jpg|thumb|A [[work of art]].]] otherwise leaves "]]" behind.
RE_MEDIA = re.compile(r"\[\[(?:File|Image|Category):[^\[\]]*\]\]", re.I)
RE_LINK_PIPED = re.compile(r"\[\[[^\[\]|]*\|([^\[\]]*)\]\]")
RE_LINK_PLAIN = re.compile(r"\[\[([^\[\]|]*)\]\]")
RE_EXTLINK = re.compile(r"\[https?://\S+\s([^\]]*)\]")
RE_EXTBARE = re.compile(r"\[https?://\S+\]")
RE_HEADING = re.compile(r"^=+\s*(.*?)\s*=+$", re.M)
RE_QUOTES = re.compile(r"'{2,}")
RE_ENTITY = re.compile(r"&[a-zA-Z]+;|&#\d+;")
RE_WS = re.compile(r"\s+")


def drop_templates(text):
    """Remove every {{...}} template, including nested and malformed ones."""
    out, depth, i, n = [], 0, 0, len(text)
    while i < n:
        pair = text[i:i + 2]
        if pair == "{{":
            depth += 1
            i += 2
        elif pair == "}}" and depth:
            depth -= 1
            i += 2
        else:
            if not depth:
                out.append(text[i])
            i += 1
    return "".join(out)


def drop_tables(text):
    """Remove every wiki table, including nested ones.

    A table runs from a line beginning "{|" to the matching "|}". Everything
    between is markup: attributes, cell separators and layout.
    """
    out, depth = [], 0
    for line in text.split("\n"):
        if RE_TABLE_OPEN.match(line):
            depth += 1
            continue
        if depth:
            if RE_TABLE_CLOSE.match(line):
                depth -= 1
            continue
        if RE_TABLE_ROW.match(line):
            continue
        out.append(line)
    return "\n".join(out)


def strip_markup(text):
    """Return `text` with MediaWiki markup removed and whitespace collapsed."""
    text = RE_COMMENT.sub(" ", text)
    text = RE_REF.sub(" ", text)
    text = RE_MATH.sub(" ", text)
    text = RE_BLOCK.sub(" ", text)
    text = drop_tables(text)
    text = drop_templates(text)
    # Innermost brackets first, repeatedly, so nested links unwrap correctly.
    for _ in range(8):
        before = text
        text = RE_MEDIA.sub(" ", text)
        text = RE_LINK_PIPED.sub(r"\1", text)
        text = RE_LINK_PLAIN.sub(r"\1", text)
        if text == before:
            break
    text = RE_EXTLINK.sub(r"\1", text)
    text = RE_EXTBARE.sub(" ", text)
    text = RE_HEADING.sub(r"\1", text)
    text = RE_TAG.sub(" ", text)
    text = RE_QUOTES.sub(" ", text)
    text = RE_ENTITY.sub(" ", text)
    # Last sweep: any filename that survived a malformed template. The
    # caption beside it is ordinary English and is kept.
    text = RE_FILETOKEN.sub(" ", text)
    return RE_WS.sub(" ", text).strip()


def articles(path):
    """Yield (title, body) for every article in the dump.

    Redirects and every namespace other than 0 (articles) are skipped: talk
    pages, templates and user pages are not prose and would skew the count.
    """
    with bz2.open(path, "rb") as handle:
        title = ns = None
        for event, elem in ET.iterparse(handle, events=("end",)):
            tag = elem.tag.rsplit("}", 1)[-1]
            if tag == "title":
                title = elem.text
            elif tag == "ns":
                ns = elem.text
            elif tag == "text":
                body = elem.text
                if ns == "0" and body and not body.lstrip().lower().startswith("#redirect"):
                    yield title, body
                elem.clear()
            elif tag == "page":
                elem.clear()


def main():
    if len(sys.argv) != 3:
        sys.exit(f"usage: {sys.argv[0]} <dump.xml.bz2> <out.txt.bz2>")
    src, dst = sys.argv[1], sys.argv[2]
    kept = skipped = 0
    with bz2.open(dst, "wt", encoding="utf-8", compresslevel=9) as out:
        for title, body in articles(src):
            text = strip_markup(body)
            # Articles shorter than 200 characters after stripping are stubs
            # and navigation pages; they add files, not words.
            if len(text) < 200:
                skipped += 1
                continue
            out.write(f"{title}\t{text}\n")
            kept += 1
            if kept % 20000 == 0:
                print(f"  {kept} articles", file=sys.stderr, flush=True)
    print(f"kept {kept} articles, skipped {skipped} short ones", file=sys.stderr)


if __name__ == "__main__":
    main()
