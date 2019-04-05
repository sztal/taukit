"""Persister classes."""
# pylint: disable=arguments-differ
from logging import getLogger
import json
from .utils import safe_print, make_path, make_filepath, slice_chunks
from .serializers import JSONEncoder


class Persister:
    """Generic persister class."""

    def __init__(self, logger=None, item_name='item'):
        """Initialization method.

        Parameters
        ----------
        logger : logging.Logger
            Logger object. Module-level logger is used if ``None``.
        item_name : str
            Item name.
        """
        self.logger = logger if logger else \
            getLogger(__name__+'.'+self.__class__.__name__)
        self.counter = 0
        self.item_name = item_name

    def persist(self, item, inc=True, inc_kws=None, **kwds):
        """Persist an item.

        item : object
            An item to persist.
        inc : bool
            Should item counter be incremented and reported.
        inc_kws : dict
            Optional ``dict`` with arguments for `inc` method.
        **kwds :
            Keyword arguments passed to `persist_item` method.
        """
        if inc:
            if inc_kws is None:
                inc_kws = {}
            self.inc(**inc_kws)
        return self.persist_item(item, **kwds)

    def persist_item(self, item, **kwds):
        raise NotImplementedError

    def load(self, **kwds):
        raise NotImplementedError

    def inc(self, print_num=True, msg="\rPersisting {item_name} no. {n}", **kwds):
        """Increment counter of processed items.

        Parameters
        ----------
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
        self.counter += 1
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

    def persist_item(self, item):
        with open(self.filepath, 'a') as f:
            f.write(json.dumps(item, cls=self.json_serializer).strip()+"\n")

    @staticmethod
    def load_file(filepath, json_decoder=None):
        with open(filepath, 'r') as f:
            for line in f:
                yield json.loads(line.strip(), cls=json_decoder)

    def load(self):
        yield from self.load_file(self.filepath, json_decoder=self.json_decoder)


class DBPersister(Persister):
    """DB persister class."""

    def __init__(self, model, batch_size=0, logger=None, item_name='item'):
        """Initialization method.

        Parameters
        ----------
        model : type
            Database model class.
        batch_size : int
            Default batch size when updating data.
            No limit if non-positive.
        """
        super().__init__(logger=logger, item_name=item_name)
        self.model = model
        self.batch_size = batch_size

    def __repr__(self):
        return "<{module}.{cls} with {model} model at {addr}>".format(
            module=self.__class__.__module__,
            cls=self.__class__.__name__,
            model=self.model.__name__,
            addr=hex(id(self))
        )

    def persist_item(self, item, **kwds):
        rec = self.model.from_dict(item, **kwds)
        rec.persist()

    def persist_many(self, items, action_hook, batch_size=None, log=True,
                     n_attempts=3, **kwds):
        """Persist many items via a bulk update.

        Parameters
        ----------
        items : iterable
            Iterable of items.
        action_hook : callable
            Called at every item in `items`. Should return an object
            corresponding to the desired bulk write operation.
        batch_size : int
            Batch size for updating. If non-positive then no limit is set.
            If ``None`` then instance attribute is used.
        log : bool
            Should update be logged.
        n_attempts : int
            Number of attempts when failing.
        **kwds :
            Keyword arguments passed to model's `persist_many` method.
        """
        # pylint: disable=broad-except
        if batch_size is None:
            batch_size = self.batch_size
        if batch_size <= 0:
            items = [items]
        else:
            items = slice_chunks(items, batch_size)
        results = {}
        for chunk in items:
            attempt = 0
            while attempt < n_attempts:
                try:
                    r = self.model.persist_many(chunk, action_hook, **kwds)
                    if log and self.logger:
                        self.logger.info("%s persisted: %s", str(self), str(r))
                    if not results:
                        results = r
                    else:
                        for k in r:
                            results[k] += r[k]
                    break
                except Exception as exc:
                    attempt += 1
                    self.logger.info("%s failed attempt %d\n%s",
                                     str(self), attempt, str(exc))
                    if attempt >= n_attempts:
                        raise exc
        return results

    def load(self, **kwds):
        yield from self.model.query(**kwds)
