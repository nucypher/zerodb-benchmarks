import bz2
import click
import path
import re

head_re = re.compile(r'\<doc id="(\d+)" url="(.*)" title="(.*)"\>')
tail = "</doc>\n"


def read_docs(dirname="wiki_sample"):
    with click.progressbar(list(path.path(dirname).walkfiles("*.bz2")), label="Creating test records") as bar:
        for fname in bar:
            f = bz2.BZ2File(fname)
            text = None
            for line in f:
                if text is None:
                    r = head_re.match(line)
                    if r:
                        docid, url, title = r.groups()
                        text = []
                else:
                    if line == tail:
                        yield {"id": docid,
                               "url": url,
                               "title": title,
                               "text": "".join(text)}
                        text = None
                    else:
                        text.append(line)
            f.close()


if __name__ == "__main__":
    for doc in read_docs():
        print doc
