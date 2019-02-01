"""Selector classes."""


class Selector:

    def __init__(self, selector):
        """Initialization method.

        Parameters
        ----------
        selector : str
            Selector string.
        """
        self.selector = selector


class CSS(Selector):
    pass

class XPath(Selector):
    pass
