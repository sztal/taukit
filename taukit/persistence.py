"""Persister classes."""
# pylint: disable=arguments-differ,protected-access
import os
from logging import getLogger
import json
from ftplib import FTP_TLS
from ftplib import all_errors
from .utils import safe_print, make_path, make_filepath, slice_chunks
from .serializers import JSONEncoder


class Persister:
    """Generic persister class."""

    def __init__(self):
        """Initialization method.

        Parameters
        ----------
        item_name : str
            Item name.
        """
        self.logger = getLogger(__name__+'.'+self.__class__.__name__)
        self.counter = 0

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

    def inc(self, print_num=True, msg="\rPersisting item no. {n}", **kwds):
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
                n=self.counter,
                **kwds
            ), nl=False)
        return self.counter


class FilePersister(Persister):
    """Generic file persister class."""

    def __init__(self, filename, dirpath):
        """Initialization method.

        Parameters
        ----------
        filename : str
            Persistence file name.
        dirpath : str
            Persistence directory path.
        """
        super().__init__()
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
                 json_decoder=None):
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

    def __init__(self, model, batch_size=0):
        """Initialization method.

        Parameters
        ----------
        model : type
            Database model class.
        batch_size : int
            Default batch size when updating data.
            No limit if non-positive.
        """
        super().__init__()
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
        rec.tk__persist()

    def persist_many(self, items, action_hook, batch_size=None, n_attempts=3, **kwds):
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
        results = None
        for chunk in items:
            attempt = 0
            while attempt < n_attempts:
                try:
                    r = self.model.tk__persist_many(chunk, action_hook, **kwds)
                    results = self.model.tk__persist_many_merge_results(
                        results, r, self.logger
                    )
                    break
                except Exception as exc:
                    attempt += 1
                    self.logger.info("%s failed attempt %d\n%s",
                                     str(self), attempt, str(exc))
                    if attempt >= n_attempts:
                        raise exc
        return results

    def query(self, **kwds):
        return self.model.tk__query(**kwds)

    def drop_data(self, **kwds):
        self.model.tk__drop(**kwds)

    def load(self, **kwds):
        yield from self.model.tk__query(**kwds)


class FTPPersister:
    """FTP persister class.

    Attributes
    ----------
    host : str
        Host string.
    user : str
        Username.
    passwd : str
        Password.
    dirpath : str
        Starting dirpath.
    ftp : ftplib FTP client class
        FTP client.
    """
    def __init__(self, host, user, passwd, dirpath=None, ftp=FTP_TLS, **kwds):
        """Initialization method."""
        super().__init__()
        self.host = host
        self.user = user
        self.passwd = passwd
        self.ftp = ftp
        self._ftp = None
        self.depth = 0
        self.set_dirpath(dirpath, create=True)
        self.ftp_kws = kwds if kwds else {}

    def __enter__(self):
        self._ftp = self.ftp(
            host=self.host,
            user=self.user,
            passwd=self.passwd,
        )

    def __exit__(self, type, value, traceback):
        # pylint: disable=redefined-builtin
        self._ftp.close()

    def move(self, dirpath, clb=None, create=True, **kwds):
        """Move through directory tree.

        Parameters
        ----------
        dirpath : str
            Target directory.
        clb : callable
            Optional callback to call after reaching the target directory.
        create : bool
            Should missing directories on the path be created if needed.
        **kwds :
            Keyword arguments passed to the callback.
        """
        def _move(dirpath, clb, create, **kwds):
            head, tail = os.path.split(dirpath)
            if head and head != '/':
                _move(head, clb=None, create=create, **kwds)
            try:
                self._ftp.cwd(tail)
            except all_errors as exc:
                if create:
                    self._ftp.mkd(tail)
                    self._ftp.cwd(tail)
                else:
                    raise exc
            if clb:
                clb(**kwds)

        with self:
            _move(dirpath, clb, create, **kwds)

    def set_dirpath(self, dirpath, create=True):
        """Set current directory path.

        Parameters
        ----------
        dirpath : str
            Proper directory path.
        create : bool
            Create missing directories if needed.
        """
        self.dirpath = dirpath
        if dirpath is not None and create:
            self.move(dirpath, create=True)

    def store(self, name, fp, dtype, **kwds):
        """Store a file.

        Parameters
        ----------
        name : str
            Filename.
        fp : file-like
            File-like obejct of a proper type.
        dtype : {'binary', 'text'}
            Data type.
        """
        if dtype.lower() == 'binary':
            self._ftp.storbinary(f"STOR {name}", fp, **kwds)
        elif dtype.lower() == 'text':
            self._ftp.storlines(f"STOR {name}", fp, **kwds)
        else:
            raise ValueError(f"incorrect data type '{dtype}'")

    def store_binary(self, name, fp, **kwds):
        """Store a binary file."""
        self.store(name, fp, dtype='binary', **kwds)

    def store_text(self, name, fp, **kwds):
        """Store a text file."""
        self.store(name, fp, dtype='text', **kwds)
