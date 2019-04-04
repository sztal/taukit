"""Persister classes."""
# pylint: disable=arguments-differ
from logging import getLogger
import json
from .utils import safe_print, make_path, make_filepath
from .serializers import JSONEncoder


class Persister:
    """Generic persister class."""

    def __init__(self, batch_size=None, logger=None, item_name='item'):
        """Initialization method.

        Parameters
        ----------
        batch_size : int or None
            Persistence batch size. No limit if ``None``.
        logger : logging.Logger
            Logger object. Module-level logger is used if ``None``.
        item_name : str
            Item name.
        """
        self.batch_size = batch_size
        self.logger = logger if logger else \
            getLogger(__name__+'.'+self.__class__.__name__)
        self.counter = 0
        self.item_name = item_name

    def persist(self, item):
        """Persist an item."""
        raise NotImplementedError

    def load(self):
        raise NotImplementedError

    def inc(self, num=1, print_num=True, msg="\rPersisting {item_name} no. {n}", **kwds):
        """Increment counter of processed items.

        Parameters
        ----------
        num : int
            Number of items add.
        print_num : bool
            Should number of processed items be printed to *stdout*.
        item_name : str
            Optional item name to overwrite instance level configuration.
        msg : str
            Formattable string with a message.
            It needs to have at least two interpolated parts with named
            `item_name` and `n`. More named interpolated parts may be used
            adn they can be supplied via `**kwds`.
        **kwds :
            Optional keyword arguments used to format the message string.
        """
        self.counter += num
        if print_num and self.counter > 1:
            safe_print(msg.format(
                item_name=self.item_name,
                n=self.counter,
                **kwds
            ), nl=False)
        return self.counter


class FilePersister(Persister):
    """Generic file persister class."""

    def __init__(self, filename, dirpath, logger=None, item_name='item'):
        """Initialization method.

        Parameters
        ----------
        filename : str
            Persistence file name.
        dirpath : str
            Persistence directory path.
        """
        super().__init__(logger=logger, item_name=item_name)
        self.filename = filename
        self.dirpath = dirpath
        self.json_serializer = JSONEncoder
        self._filepath = None

    @property
    def filepath(self):
        """Filepath getter."""
        if not self._filepath:
            self._filepath = make_path(
                make_filepath(self.filename, self.dirpath, inc_if_taken=True),
                create_dir=True
            )
        return self._filepath

    @staticmethod
    def load_file(filepath):
        """Load data from a file."""
        raise NotImplementedError

    def load(self):
        """Load data saved to a file."""
        raise NotImplementedError


class JSONLinesPersister(FilePersister):
    """JSON lines based file persister."""

    def __init__(self, filename, dirpath, json_encoder=JSONEncoder,
                 json_decoder=None, logger=None, item_name='item'):
        """Initialization method.

        Parameters
        ----------
        filename : str
            Persistence file name.
        dirpath : str
            Persistence directory path.
        json_encoder : JSONEncoder
            JSON encoder class.
        json_decoder : JSONDecoder
            JSON decoder class.
        """
        super().__init__(
            filename=filename,
            dirpath=dirpath,
            logger=logger,
            item_name=item_name
        )
        self.json_encoder = json_encoder
        self.json_decoder = json_decoder

    def persist(self, item):
        with open(self.filepath, 'a') as f:
            f.write(json.dumps(item, cls=self.json_serializer).strip()+"\n")

    @staticmethod
    def load_file(filepath, json_decoder=None):
        with open(filepath, 'r') as f:
            for line in f:
                yield json.loads(line.strip(), cls=json_decoder)

    def load(self):
        yield from self.load_file(self.filepath, json_decoder=self.json_decoder)
