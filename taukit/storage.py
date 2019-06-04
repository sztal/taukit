"""Storage.

This module provides classes for handling various types of storage and data
persistence such as file, FTP, S3, Dropbox or database storage.
"""
#pylint: disable=arguments-differ,protected-access
import os
from logging import getLogger
import json
from ftplib import FTP_TLS
from ftplib import all_errors
from pymongo import UpdateOne
from .utils import safe_print, make_path, make_filepath, slice_chunks
from .serializers import JSONEncoder


class Storage:
    """Base storage class.

    Attributes
    ----------
    log_messages : bool
        Should messages be logged.
    print_messages : bool
        Should messages be printed.
    logger : logging.Logger
        Logger obejct.
    item_name : str
        Item name to use in messages.
    """
    def __init__(self, log_messages=False, print_messages=True,
                 item_name='item', count_items=True):
        """Initialization method."""
        self.logger = \
            getLogger(self.__class__.__module__+'.'+self.__class__.__name__)
        self._items_counter = 0
        self.log_messages = log_messages
        self.print_messages = print_messages
        self.item_name = item_name
        self.count_items = count_items

    def message(self):
        """Message item processing."""
        if not self.log_messages and not self.print_messages:
            return
        if self._items_counter > 0:
            msg = f"Processing {self.item_name} no. {self._items_counter} ..."
        else:
            msg = f"Processing {self.item_name}s ..."
        if self.print_messages:
            safe_print("\r"+msg, nl=False)
        if self.log_messages:
            self.logger.info(msg)

    def pipe_item(self, item):
        """Pipe item and report item count."""
        if self.count_items:
            self._items_counter += 1
            self.message()
        return item

    def save(self, item, **kwds):
        """Save item to storage.

        Parameters
        ----------
        item : object
            Object to save.
        **kwds :
            Keyword arguments passed to the `save_item` method.
        """
        item = self.pipe_item(item)
        return self.save_item(item, **kwds)

    def save_item(self, item):
        """Implementation of item save.

        This is the method that implements the actual item
        saving logic in particula types of `Storage` classes.
        Hence, this is the method that needs to be extended in subclasses.

        Parameters
        ----------
        item : object
            Object to save.
        """
        raise NotImplementedError

    def load(self, **kwds):
        """Load from storage.

        This is an abstract method that needs to be defined on subclasses.
        """
        raise NotImplementedError


# File storage ----------------------------------------------------------------

class FileStorage(Storage):
    # pylint: disable=abstract-method
    """File storage class.

    Attributes
    ----------
    filename : str
        Filename.
    dirpath : str
        Path to the directory where the file should be.
    if_exists : {'inc', 'increment', 'raise'}
        What to do if the specified filepath is already taken.
        If ``'inc'`` or ``'increment'`` then counting number is added
        to the filename. If ``'raise'`` then `FileExsistsError` is raised.
    create_dir : bool
        Should directories for file be created if needed.
    """
    def __init__(self, filename, dirpath, if_exists='inc', create_dir=True, **kwds):
        """Initialization method."""
        super().__init__(**kwds)
        self.filename = filename
        self.dirpath = dirpath
        self.if_exists = if_exists
        self.create_dir = create_dir
        self._filepath = None

    @property
    def filepath(self):
        if not self._filepath:
            inc = self.if_exists.lower() in ('inc', 'increment')
            fp = make_filepath(self.filename, self.dirpath, inc_if_taken=inc)
            if self.if_exists.lower() == 'raise' and os.path.exists(fp):
                raise FileExistsError(f"'{fp}' already exists'")
            self._filepath = make_path(fp, create_dir=self.create_dir)
        return self._filepath


class JSONLinesStorage(FileStorage):
    """JSON lines based file storage.

    Attributes
    ----------
    filename : str
        Filename.
    dirpath : str
        Path to the directory where the file should be.
    json_encoder : json.JSONEncoder
        JSON encoder class.
    json_decoder : json.JSONDecoder
        JSON decoder class.
    if_exists : {'inc', 'increment', 'raise', 'append'}
        What to do if the specified filepath is already taken.
        If ``'inc'`` or ``'increment'`` then counting number is added
        to the filename. If ``'raise'`` then `FileExsistsError` is raised.
        Append to the file if ``'append'``.
    create_dir : bool
        Should directories for file be created if needed.
    """
    def __init__(self, filename, dirpath, json_encoder=JSONEncoder,
                 json_decoder=None, if_exists='inc', create_dir=True, **kwds):
        """Initialization method."""
        super().__init__(
            filename=filename,
            dirpath=dirpath,
            if_exists=if_exists,
            create_dir=create_dir,
            **kwds
        )
        self.json_encoder = json_encoder
        self.json_decoder = json_decoder

    @property
    def filepath(self):
        try:
            return super().filepath
        except FileExistsError as exc:
            if self.if_exists.lower() == 'append':
                return os.path.join(self.dirpath, self.filename)
            raise exc

    def save_item(self, item):
        with open(self.filepath, 'a') as f:
            f.write(json.dumps(item, cls=self.json_encoder).strip()+"\n")

    @staticmethod
    def load_jl(filepath, mode='r', json_decoder=None):
        """Load JSON-lines file.

        Parameters
        ----------
        filepath : str
            Filepath.
        mode : str
            File open mode.
        json_decoder:
            JSON decoder class.
        """
        with open(filepath, mode) as f:
            for line in f:
                yield json.loads(line.strip(), cls=json_decoder)

    def load(self, mode='r'):
        yield from self.load_jl(self.filepath, mode=mode,
                                json_decoder=self.json_decoder)


# Database storage ------------------------------------------------------------

class DBStorage(Storage):
    # pylint: disable=abstract-method
    """Database storage.

    It is designed to interact with class-based ORM/ODM models such as those
    provided by *SQLAlchemy* or *MongoEngine*.

    Attributes
    ----------
    model : type
        Database model.
    batch_size : int
        Default batch size when making bulk updates.
        No limit if non-positive or falsy.
    updater : callable
        Default updater function for making bulk updates.
        If not provided then class default method is used.
    processor : callable
        Optional callable for preprocessing items
        before turning them into update statements.
    """
    def __init__(self, model, batch_size=None, updater=None, processor=None, **kwds):
        """Initialization method."""
        super().__init__(**kwds)
        self.model = model
        self.batch_size = batch_size
        self._updater = updater
        self.processor = processor

    def __repr__(self):
        module = self.__class__.__module__
        clsname = self.__class__.__name__
        modelname = self.model.__name__
        return f"<{module}.{clsname} for '{modelname}' at {hex(id(self))}>"

    def updater(self, item):
        """Convert item to bulk update action."""
        item = self.pipe_item(item)
        if self.processor:
            item = self.processor(item)
        if self._updater:
            item = self._updater(item)
        return item

    def make_bulk_update(self, items, **kwds):
        """Perform bulk update.

        This is an abstract method that needs to be defined on subclasses.

        Parameters
        ----------
        items : iterable of objects
            Items to update.
        """
        raise NotImplementedError

    def format_bulk_update_results(self, results, new_result):
        """Format and merge bulk update results.

        Parameters
        ----------
        results : object
            Results object.
        new_result : object
            New results object.
        """
        if results is None:
            results = []
        results.append(new_result)
        return results

    def bulk_update(self, items, batch_size=None, n_attempts=3, **kwds):
        """Handle bulk update.

        Parameters
        ----------
        items : iterable of objects
            Items to update.
        batch_size : int
            Optional batch size. If ``None`` then instance attribute is used.
        n_attempts : int
            Number of update attempts before failing.
            This is useful when there may be connection errors.
        **kwds :
            Keyword parameters passed to `make_bulk_update`, which implements
            the database specific logic.
        """
        batch_size = batch_size if batch_size is not None else self.batch_size
        if not batch_size or batch_size <= 0:
            items = [items]
        else:
            items = slice_chunks(items, batch_size)
        results = None
        for batch in items:
            attempt = 0
            while attempt < n_attempts:
                mname = self.model.__name__
                batch = [ self.updater(item) for item in batch ]
                try:
                    res = self.make_bulk_update(batch, **kwds)
                    results = self.format_bulk_update_results(results, res)
                    msg = f"Bulk update '{mname}' {results}"
                    self.logger.info(msg)
                    break
                # pylint: disable=broad-except
                except Exception as exc:
                    attempt += 1
                    msg = f"Failed to bulk update '{mname}' #{attempt} time"
                    self.logger.exception(msg)
                    if attempt >= n_attempts:
                        raise exc
        return results


class MongoStorage(DBStorage):
    """_MongoDB_ storage.

    Attributes
    ----------
    model : type
        Database model.
    batch_size : int
        Default batch size when making bulk updates.
        No limit if non-positive or falsy.
    updater : callable
        Optional default updater function for making bulk updates.
    """
    def __init__(self, model, batch_size=0, updater=None,
                 processor=None, item_name='document', **kwds):
        """Initialization method."""
        super().__init__(
            model=model,
            batch_size=batch_size,
            updater=updater,
            processor=processor,
            item_name=item_name,
            **kwds
        )

    def updater(self, item, upsert=True, **kwds):
        """Convert document to _PyMongo_ update action.

        Parameters
        ----------
        item : dict-like
            Document.
        upsert : bool
            Upsert flag for :py:class:`pymongo.UpdateOne`.
        **kwds :
            Keyword arguments passed to :py:class:`pymongo.UpdateOne` constructor.
        """
        item = self.model.from_dict(item, only_dict=True)
        item = super().updater(item)
        if not self._updater:
            _id = self.model.id.name
            item = UpdateOne(
                filter={ _id: item.pop(_id) },
                update={ '$set': item },
                upsert=upsert,
                **kwds
            )
        return item

    def save_item(self, item, **kwds):
        """Save an item.

        Parameters
        ----------
        item : dict-like
            A dict-like object.
        **kwds :
            Keyword arguments passed to :py:meth:`mongoengine.Document.save`.
        """
        self.model(**item).save(**kwds)

    def load(self, **kwds):
        """Load items from the collection.

        Parameters
        ----------
        **kwds :
            Keyword arguments passed to _MongoEngine_ `objects`.
        """
        self.model.objects(**kwds)

    def make_bulk_update(self, items, **kwds):
        """Make bulk update.

        Parameters
        ----------
        items : iterable
            Proper _PyMongo_ update action objects.
        **kwds :
            Keyword arguments passed to _PyMongo_ `bulk_write`.
        """
        res = self.model._get_collection().bulk_write(items, **kwds)
        return res

    def format_bulk_update_results(self, results, new_result):
        """Format and merge bulk update results."""
        if not results:
            results = {
                'writeErrors': [],
                'writeConcernErrors': [],
                'nInserted': 0,
                'nUpserted': 0,
                'nMatched': 0,
                'nModified': 0,
                'nRemoved': 0
            }
        for k in results:
            results[k] += new_result.bulk_api_result[k]
        return results


# Other storage ---------------------------------------------------------------

class FTPStorage(Storage):
    """FTP storage class.

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
    ftp_kws : dict
        Other keyword parameters passed to the FTP client constructor.
    """
    def __init__(self, host, user, passwd, dirpath=None,
                 ftp=FTP_TLS, ftp_kws=None, **kwds):
        """Initialization method."""
        super().__init__(**kwds)
        self.host = host
        self.user = user
        self.passwd = passwd
        self.ftp = ftp
        self._ftp = None
        self.set_dirpath(dirpath, create=True)
        self.ftp_kws = ftp_kws if ftp_kws else {}

    def __enter__(self):
        self._ftp = self.ftp(
            host=self.host,
            user=self.user,
            passwd=self.passwd,
            **self.ftp_kws
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

    def save_item(self, item, name, dtype, **kwds):
        """Save item in FTP.

        Parameters
        ----------
        item : file-like
            Object in a file-like form.
        name : str
            Name of the object to save.
        dtype : {'text', 'binary'}
            Data type of the item to save.
        **kwds :
            Keyword arguments passed to STOR FTP actions.
        """
        action = f"STOR {name}"
        if dtype.lower() == 'binary':
            self._ftp.storbinary(action, item, **kwds)
        if dtype.lower() == 'text':
            self._ftp.storlines(action, item, **kwds)
        else:
            raise ValueError(f"incorrect data type '{dtype}'")

    def save(self, item, name, dtype, **kwds):
        """See :py:meth:`taukit.storage.FTPStorage.save_item`."""
        return self.save_item(item, name, dtype, **kwds)

    def load(self, name, dtype):
        """Load item from FTP."""
        raise NotImplementedError
