"""The smallest Flyte workflow that shows a value crossing a task boundary.

Run it on the sandbox:

    pyflyte run --remote hello.py count_words --sentence "the quick brown fox"

Two tasks, one workflow. `split` returns a list of strings and `tally` takes
one; Flyte checks that those two types agree before anything runs, which is the
difference between this and a shell script that discovers the mismatch at 3 a.m.
"""
from typing import List

from flytekit import task, workflow


@task
def split(sentence: str) -> List[str]:
    """Return the words of `sentence`, lowercased."""
    return sentence.lower().split()


@task
def tally(words: List[str]) -> int:
    """Return how many words there are."""
    return len(words)


@workflow
def count_words(sentence: str = "the quick brown fox") -> int:
    """Split a sentence and count its words."""
    return tally(words=split(sentence=sentence))
