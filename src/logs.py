import logging
import sys
from typing import TextIO

# the vocabulary a run's lines were already written in, kept as the name of the
# logger that says them: a bracketed tag became a name, and the formatter
# brackets it again
FORMAT = "[%(name)s] %(levelname)-7s %(message)s"

# what a run says at each verbosity, quietest first. Verbosity zero is the voice
# a run has when it is left to itself, which is the one a person at a terminal
# wants; quiet is for a cron run, where the pool that run leaves behind is the
# point and the lines that got there are not
LEVELS = (logging.WARNING, logging.INFO, logging.DEBUG)


class StderrHandler(logging.StreamHandler[TextIO]):
    """A handler that writes wherever stderr points when it writes, rather than
    wherever it pointed when the handler was made: a run whose output has been
    redirected deserves to have that redirection followed"""

    @property
    def stream(self) -> TextIO:
        return sys.stderr

    @stream.setter
    def stream(self, value: TextIO) -> None:
        pass


# one handler, shared by every logger a run speaks through, so verbosity is a
# single decision rather than one per tag
_handler = StderrHandler()
_handler.setFormatter(logging.Formatter(FORMAT))


def level_for(verbosity: int) -> int:
    """The level a run at this verbosity speaks at"""

    return LEVELS[max(0, min(verbosity + 1, len(LEVELS) - 1))]


def get_logger(tag: str) -> logging.Logger:
    """The logger for one part of a run, named for that part. It answers to the
    handler above and to nobody else, since a library's own log lines are not
    part of the vocabulary a run speaks in"""

    logger = logging.getLogger(tag)
    # the level on the handler is what decides what a run says out loud, so the
    # logger itself takes everything and passes it on
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if _handler not in logger.handlers:
        logger.addHandler(_handler)

    return logger


def configure(verbosity: int) -> None:
    """Let through as much as the command line asked for, and no more"""

    _handler.setLevel(level_for(verbosity))
